// Copyright 2025 The MOQtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use bytes::Bytes;
use moqtail::model::common::location::Location;
use moqtail::model::common::pair::KeyValuePair;
use moqtail::model::common::tuple::{Tuple, TupleField};
use moqtail::model::control::client_setup::ClientSetup;
use moqtail::model::control::constant::{self, GroupOrder};
use moqtail::model::control::control_message::ControlMessage;
use moqtail::model::control::fetch::{Fetch, StandAloneFetchProps};
use moqtail::model::control::publish_namespace::PublishNamespace;
use moqtail::model::control::subscribe::Subscribe;
use moqtail::model::control::subscribe_ok::SubscribeOk;
use moqtail::model::control::unsubscribe::Unsubscribe;
use moqtail::model::data::object::Object;
use moqtail::model::data::subgroup_header::SubgroupHeader;
use moqtail::model::data::subgroup_object::SubgroupObject;
use moqtail::model::error::TerminationCode;
use moqtail::transport::control_stream_handler::ControlStreamHandler;
use moqtail::transport::data_stream_handler::{
  FetchRequest, HeaderInfo, RecvDataStream, SendDataStream,
};
use std::cmp;
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::error;
use tracing::info;
use wtransport::{ClientConfig, Endpoint};

use gstreamer::prelude::*;
use gstreamer::{self as gst};
use gstreamer_app as gst_app;

#[derive(Clone)]
pub(crate) struct Client {
  pub endpoint: String,
  pub client_mode: String,
  pub validate_cert: bool,
  control_stream_handler: Option<Arc<Mutex<ControlStreamHandler>>>,
  message_notify: Arc<Notify>,
  message_queue: Arc<RwLock<VecDeque<ControlMessage>>>,
  continue_publishing: Arc<Mutex<bool>>,
}

impl Client {
  pub fn new(endpoint: String, client_mode: String, validate_cert: bool) -> Self {
    Client {
      endpoint,
      client_mode,
      validate_cert,
      control_stream_handler: None,
      message_notify: Arc::new(Notify::new()),
      message_queue: Arc::new(RwLock::new(VecDeque::new())),
      continue_publishing: Arc::new(Mutex::new(true)),
    }
  }

  pub async fn run(&mut self) -> Result<(), anyhow::Error> {
    let endpoint = self.endpoint.clone();
    let client_mode = self.client_mode.clone();
    let validate_cert = self.validate_cert;

    let c = ClientConfig::builder().with_bind_default();
    let config = if validate_cert {
      c.with_no_cert_validation().build()
    } else {
      c.with_native_certs().build()
    };

    let connection = Arc::new(
      Endpoint::client(config)
        .unwrap()
        .connect(endpoint.as_str())
        .await
        .unwrap(),
    );

    let (send_stream, recv_stream) = connection.open_bi().await.unwrap().await.unwrap();

    let mut control_stream_handler = ControlStreamHandler::new(send_stream, recv_stream);

    let client_setup = ClientSetup::new([constant::DRAFT_14].to_vec(), [].to_vec());

    match control_stream_handler.send_impl(&client_setup).await {
      Ok(_) => info!("Client setup sent successfully"),
      Err(e) => error!("Failed to send client setup: {:?}", e),
    }

    let server_setup = match control_stream_handler.next_message().await {
      Ok(ControlMessage::ServerSetup(m)) => m,
      Ok(m) => {
        error!("Unexpected message type: {:?}", m);
        return Err(anyhow::anyhow!("Unexpected message type: {:?}", m));
      }
      Err(e) => {
        error!("Failed to receive server setup: {:?}", e);
        return Err(anyhow::anyhow!("Failed to receive server setup: {:?}", e));
      }
    };

    info!("Received server setup: {:?}", server_setup);

    // compare the server setup with the client setup
    if server_setup.selected_version != constant::DRAFT_14 {
      error!(
        "Server setup version mismatch: expected {:0X}, got {}",
        constant::DRAFT_14,
        server_setup.selected_version
      );
      return Err(anyhow::anyhow!(
        "Server setup version mismatch: expected {:0X}, got {}",
        constant::DRAFT_14,
        server_setup.selected_version
      ));
    }

    self.control_stream_handler = Some(Arc::new(Mutex::new(control_stream_handler)));

    if client_mode == "publisher" {
      self.start_publisher(connection.clone()).await;
    } else if client_mode == "subscriber" || client_mode == "fetcher" {
      self
        .start_subscriber(connection.clone(), client_mode == "fetcher")
        .await;
    } else {
      error!("Invalid client mode: {}", client_mode);
      return Err(anyhow::anyhow!("Invalid client mode: {}", client_mode));
    }
    Ok(())
  }

  async fn start_publisher(&self, connection: Arc<wtransport::Connection>) {
    info!("Starting publisher...");

    gst::init().expect("Failed to initialize GStreamer");
    // === Create GStreamer pipeline ===
    let pipeline = gst::parse::launch(
      "videotestsrc pattern=ball ! video/x-raw, framerate=30/1 ! videoconvert ! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=30 ! video/x-h264,stream-format=avc,alignment=au ! mp4mux streamable=true fragment-duration=33 ! appsink name=sink emit-signals=true max-buffers=5 drop=true"
    ).unwrap();

    let sink = pipeline
      .clone()
      .dynamic_cast::<gst::Bin>()
      .unwrap()
      .by_name("sink")
      .unwrap()
      .dynamic_cast::<gst_app::AppSink>()
      .unwrap();

    let my_namespace = Tuple::from_utf8_path("moqtail");

    let request_id = 0;

    // start by sending an announce message
    self
      .publish_namespace_and_wait(request_id, my_namespace.clone())
      .await
      .unwrap();

    info!("PublishNamespace sent successfully");

    // wait for subscribe or fetch, enter loop
    loop {
      let message;
      {
        let control_stream_handler = self.control_stream_handler.clone().unwrap();
        let mut control = control_stream_handler.lock().await;
        message = control.next_message().await;
      }

      match message {
        Ok(ControlMessage::Subscribe(m)) => {
          info!("Received subscribe message: {:?}", m);
          // Handle the subscribe message
          // send Subscribe_ok
          let track_alias = self.send_subscribe_ok(m.request_id).await;

          // open a unidirectional stream
          let connection = connection.clone();
          let continue_publishing = self.continue_publishing.clone();
          let sink_clone = sink.clone();
          pipeline.set_state(gst::State::Playing).unwrap();
          tokio::spawn(async move {
            let mut file = std::fs::File::create("dump.pub").expect("Could not create dump file");
            for group_id in 1..2 {
              let continue_publishing = continue_publishing.lock().await;
              if !*continue_publishing {
                info!("Stopping publishing");
                break;
              }

              info!("Opening unidirectional stream for group_id: {}", group_id);
              let stream = connection.open_uni().await.unwrap().await.unwrap();
              let sub_header =
                SubgroupHeader::new_with_explicit_id(track_alias, group_id, 1u64, 1u8, true, true);

              let header_info = HeaderInfo::Subgroup {
                header: sub_header,
                /*subscribe_request: *m.clone(),*/
              };
              let stream = Arc::new(Mutex::new(stream));
              let stream_handler = SendDataStream::new(stream.clone(), header_info).await;

              match stream_handler {
                Ok(mut handler) => {
                  let mut prev_object_id = None;
                  for object_id in 0..8 {
                    info!("Sending object: object_id: {}", &object_id);

                    match sink_clone.pull_sample() {
                      Ok(sample) => {
                        let buffer = sample.buffer().unwrap();
                        let map = buffer.map_readable().unwrap();
                        let data = Bytes::copy_from_slice(map.as_slice());

                        if let Err(e) = file.write_all(&data) {
                          error!("Failed to write to dump file: {:?}", e);
                        }

                        let object = SubgroupObject {
                          object_id,
                          extension_headers: Some(vec![]),
                          object_status: None,
                          payload: Some(Bytes::from(data)),
                        };
                        let object = Object::try_from_subgroup(
                          object,
                          track_alias,
                          group_id,
                          Some(group_id),
                          1,
                        )
                        .unwrap();

                        match handler.send_object(&object, prev_object_id).await {
                          Ok(_) => info!("Object sent successfully - i: {}", &object_id),
                          Err(e) => error!("Failed to send object: {:?}", e),
                        }
                      }
                      Err(_) => {
                        error!("Stream input ended");
                      }
                    }

                    prev_object_id = Some(object_id);
                  }
                  // TODO: normally, we need to finish the stream but the peer does not
                  // acknowledge it and we get stuck in the loop
                  handler.flush().await.unwrap();
                  info!("Unidirectional stream flushed for group_id: {}", group_id);
                  handler.finish().await.unwrap();
                  //info!("Unidirectional stream finished for group_id: {}", group_id);
                }
                Err(e) => {
                  error!("Failed to open unidirectional stream: {:?}", e);
                }
              }
            }
          });

          info!("Subscribe ok sent successfully");
        }
        Ok(ControlMessage::Unsubscribe(m)) => {
          info!("Received unsubscribe message: {:?}", m);
          // stop publishing
          let mut continue_publishing = self.continue_publishing.lock().await;
          *continue_publishing = false;
          info!("Stopped publishing");
        }
        Ok(_) => {
          error!("Unexpected message type");
        }
        Err(e) => {
          error!("Failed to receive message: {:?}", e);
          break;
        }
      }
    }
  }

  async fn start_subscriber(&self, connection: Arc<wtransport::Connection>, is_fetcher: bool) {
    info!("Starting subscriber...");

    gst::init().expect("Failed to initialize GStreamer.");

    // Receiver jitter buffer. The sender sometimes may send the video in bursts
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(100);

    let pipeline_str = "appsrc name=videosrc ! queue ! qtdemux name=demux demux.video_0 ! h264parse ! avdec_h264 ! videoconvert ! autovideosink";
    let pipeline = gst::parse::launch(pipeline_str).expect("Failed to create GStreamer pipeline.");

    // // GSTREAMER: 3. Get a handle to the `appsrc` element so we can push data into it.
    let appsrc = pipeline
      .clone()
      .dynamic_cast::<gst::Bin>()
      .unwrap()
      .by_name("videosrc")
      .unwrap()
      .dynamic_cast::<gst_app::AppSrc>()
      .unwrap();

    // // ---- Set caps ----
    let caps_str = "video/quicktime, variant=(string)iso";
    let caps = gst::Caps::from_str(caps_str).unwrap();
    appsrc.set_caps(Some(&caps));

    // GSTREAMER: 4. Set the pipeline to the "Playing" state. This will create the window.
    pipeline
      .set_state(gst::State::Playing)
      .expect("Failed to set pipeline to playing");

    // TODO: for this demo, we don't pass those
    let pending_fetches = Arc::new(RwLock::new(BTreeMap::new()));
    let conn = connection.clone();
    let pending_fetches_2 = pending_fetches.clone();

    // Still in `start_subscriber`, after creating the pipeline and getting `appsrc`...
    let appsrc_clone = appsrc.clone();

    tokio::spawn(async move {
      // Inside your tokio::spawn, before the loop
      let mut file = std::fs::File::create("dump.sub").expect("Could not create dump file");

      'group_loop: loop {
        info!("Waiting for incoming unidirectional streams...");
        // listen for incoming unidirectional streams
        let stream = conn.accept_uni().await.unwrap();
        info!("Accepted unidirectional stream");

        let mut stream_handler = &RecvDataStream::new(stream, pending_fetches_2.clone());

        loop {
          let next = stream_handler.next_object().await;
          match next {
            (handler, Some(object)) => {
              info!("Received object: {:?}", object);
              // Handle the object
              if let Some(payload) = object.payload {
                // Also write the payload to a file
                if let Err(e) = file.write_all(&payload) {
                  error!("Failed to write to dump file: {:?}", e);
                }
                if let Err(e) = tx.send(payload).await {
                  error!("Failed to send object to buffer channel: {}. Stopping.", e);
                  break 'group_loop; // Break the outer loop if the channel is closed.
                }
              }
              stream_handler = handler;
            }
            (_, None) => {
              // error!("Failed to receive object: {:?}", e);
              info!("No more objects in the stream, closing...");
              break;
            }
          }
        }
      }
    });

    tokio::spawn(async move {
      let mut start_time = SystemTime::now();
      // 6 frames every buffer fill. 198 ms => 22
      // 3 frames every buffer fill. 99 ms => 15
      let feed_speed = 22;
      loop {
        // Asynchronously wait until an object is available in the buffer.
        // This will return `None` if the sender side of the channel is closed.
        match rx.recv().await {
          Some(payload) => {
            let gst_buffer = gst::Buffer::from_slice(payload);

            let elapsed_time = SystemTime::now().duration_since(start_time).unwrap();
            if elapsed_time.as_millis() < feed_speed {
              let sleep_time =
                std::time::Duration::from_millis(feed_speed.try_into().unwrap()) - elapsed_time;
              info!("Sleeping a little {:?}", sleep_time);
              // tokio::time::sleep(sleep_time).await;
            } else {
              info!("Elapsed {:?}", elapsed_time);
            }

            info!("Pushing to gstreamer");

            if let Err(flow_return) = appsrc_clone.push_buffer(gst_buffer) {
              error!("GStreamer feeder failed to push buffer: {:?}", flow_return);
              // If the pipeline is closed, stop this task.
              if flow_return == gst::FlowError::Flushing || flow_return == gst::FlowError::Eos {
                break;
              }
            }
            start_time = SystemTime::now();
          }
          None => {
            // Channel was closed, no more objects will ever arrive.
            info!("Buffer channel closed, stopping GStreamer feeder task.");
            break;
          }
        }
      }
    });

    if !is_fetcher {
      // sub and unsub test
      // send subscribe once in 4 seconds in another task continuously
      let this = self.clone();
      tokio::spawn(async move {
        // loop {
        let sub = this.send_subscribe(0).await;
        info!("Subscribe sent successfully: {:?}", sub);
      });
    } else {
      let this = self.clone();
      let pending_fetches = pending_fetches.clone();
      tokio::spawn(async move {
        this.send_fetch(0, pending_fetches).await;
      });
    }

    loop {
      info!("Waiting for next message...");
      let control_stream_handler = self.control_stream_handler.clone().unwrap();
      let mut control_stream_handler = control_stream_handler.lock().await;

      let msg_result: Option<Result<ControlMessage, TerminationCode>>;

      // Check if there's a message in the queue first
      tokio::select! {
        _ =  self.message_notify.notified() => {
          info!("Message notified");
          let mut queue = self.message_queue.write().await;
          while let Some(queued_msg) = queue.pop_front() {
            control_stream_handler.send(&queued_msg).await.unwrap();
          }
          msg_result = None;
        }
        res = control_stream_handler.next_message() => {
          info!("Message received from control stream");
          msg_result = Some(res);
        }
        else => {
          info!("No message notified");
          msg_result = None;
        }
      }

      if let Some(msg_result) = msg_result {
        match msg_result {
          Ok(ControlMessage::SubscribeOk(m)) => {
            info!("Received SubscribeOk message: {:?}", m);
            // Handle the subscribe message
            // send Subscribe_ok
            // send_subscribe_ok(control_stream_handler, m.subscribe_id).await;
          }
          Ok(ControlMessage::Unsubscribe(m)) => {
            info!("Received unsubscribe message: {:?}", m);
            // Handle the unsubscribe message
          }
          Ok(ControlMessage::FetchOk(m)) => {
            info!("Received fetch ok message: {:?}", m);
            // Handle the fetch ok message
          }
          Ok(ControlMessage::FetchError(m)) => {
            info!("Received fetch error message: {:?}", m);
            // Handle the fetch error message
          }
          Ok(_) => {
            error!("Unexpected message type");
          }
          Err(e) => {
            error!("Failed to receive message: {:?}", e);
            break;
          }
        }
      }
    }
  }

  async fn send_subscribe(&self, request_id: u64) -> Subscribe {
    let track_namespace = Tuple::from_utf8_path("/moqtail");
    let track_name = "demo".to_string();
    let subscriber_priority = 1;
    let group_order = GroupOrder::Ascending;
    let forward = true;
    let subscribe_parameters = vec![];
    let sub = Subscribe::new_latest_object(
      request_id,
      track_namespace,
      track_name,
      subscriber_priority,
      group_order,
      forward,
      subscribe_parameters,
    );
    self
      .message_queue
      .write()
      .await
      .push_back(ControlMessage::Subscribe(Box::new(sub.clone())));
    self.message_notify.notify_waiters();
    sub
  }

  async fn send_fetch(
    &self,
    request_id: u64,
    pending_fetches: Arc<RwLock<BTreeMap<u64, FetchRequest>>>,
  ) {
    info!("Starting fetcher...");
    let mut track_namespace = Tuple::new();
    track_namespace.add(TupleField::from_utf8("moqtail"));
    let track_name = "demo".to_string();
    let start_location = Location::new(1, 0);
    let end_location = Location::new(5, 3);
    let parameters = vec![KeyValuePair::try_new_varint(100, 200).unwrap()];
    let standalone_fetch_props = StandAloneFetchProps {
      track_namespace,
      track_name,
      start_location,
      end_location,
    };

    let fetch = Fetch::new_standalone(
      request_id,
      1,
      GroupOrder::Ascending,
      standalone_fetch_props,
      parameters.clone(),
    );
    info!("Fetch message: {:?}", fetch);
    self
      .message_queue
      .write()
      .await
      .push_back(ControlMessage::Fetch(Box::new(fetch.clone())));
    self.message_notify.notify_waiters();
    pending_fetches
      .write()
      .await
      .insert(request_id, FetchRequest::new(request_id, 1, fetch, 0));

    info!("Fetch message sent successfully");
  }

  async fn send_subscribe_ok(&self, request_id: u64) -> u64 {
    info!("Sending SubscribeOk message");
    let track_alias = 1u64;
    let control_stream_handler = self.control_stream_handler.clone().unwrap();
    let mut control_stream_handler = control_stream_handler.lock().await;
    info!("Control stream handler locked");
    let msg = SubscribeOk::new_ascending_with_content(request_id, track_alias, 0, None, None);
    control_stream_handler.send_impl(&msg).await.unwrap();
    info!("SubscribeOk message sent successfully");
    track_alias
  }

  async fn send_unsubscribe(&self, request_id: u64) {
    let msg = Unsubscribe::new(request_id);
    info!("Sending unsubscribe message: {:?}", msg);
    self
      .message_queue
      .write()
      .await
      .push_back(ControlMessage::Unsubscribe(Box::new(msg)));
    self.message_notify.notify_waiters();
  }

  async fn publish_namespace_and_wait(
    &self,
    request_id: u64,
    my_namespace: Tuple,
  ) -> Result<(), anyhow::Error> {
    let control_stream_handler = self.control_stream_handler.clone().unwrap();
    let mut control_stream_handler = control_stream_handler.lock().await;
    // send announce, request id 0
    let announce = PublishNamespace::new(request_id, my_namespace, &[]);
    control_stream_handler.send_impl(&announce).await.unwrap();

    let announce_ok = control_stream_handler.next_message().await;
    match announce_ok {
      Ok(ControlMessage::PublishNamespaceOk(m)) => {
        info!("Received announce ok message: {:?}", m);
        Ok(())
      }
      Ok(_) => {
        error!("Expecting announce ok message");
        Err(anyhow::anyhow!("Expecting announce ok message"))
      }
      Err(e) => {
        // TODO: request id mismatch should be handled in control stream handler
        error!("Failed to receive message: {:?}", e);
        Err(anyhow::anyhow!("Failed to receive message: {:?}", e))
      }
    }
  }
}
