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

use super::constant::{ControlMessageType, FilterType, GroupOrder};
use super::control_message::ControlMessageTrait;
use crate::model::common::location::Location;
use crate::model::common::pair::KeyValuePair;
use crate::model::common::tuple::{Tuple, TupleField};
use crate::model::common::varint::{BufMutVarIntExt, BufVarIntExt};
use crate::model::data::full_track_name::FullTrackName;
use crate::model::error::ParseError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const PARAM_FORWARD: u64 = 0x10;
pub const PARAM_SUBSCRIBER_PRIORITY: u64 = 0x20;
pub const PARAM_SUBSCRIPTION_FILTER: u64 = 0x21;
pub const PARAM_GROUP_ORDER: u64 = 0x22;

fn build_filter_bytes(
  filter_type: FilterType,
  start_location: Option<Location>,
  end_group: Option<u64>,
) -> Result<Bytes, ParseError> {
  let mut payload = BytesMut::new();
  payload.put_vi(filter_type)?;

  match filter_type {
    FilterType::AbsoluteStart => {
      if let Some(ref loc) = start_location {
        payload.extend_from_slice(&loc.serialize()?);
      } else {
        unreachable!()
      }
    }
    FilterType::AbsoluteRange => {
      if let Some(ref loc) = start_location {
        payload.extend_from_slice(&loc.serialize()?);
      }
      if let Some(eg) = end_group {
        payload.put_vi(eg)?;
      }
    }
    _ => {}
  }
  Ok(payload.freeze())
}

#[derive(Debug, PartialEq, Clone)]
pub struct Subscribe {
  pub request_id: u64,
  pub track_namespace: Tuple,
  pub track_name: TupleField,
  // TODO: make the following optional
  pub subscribe_parameters: Vec<KeyValuePair>,
}

#[allow(clippy::too_many_arguments)]
impl Subscribe {
  /// Creates a basic Draft-16 Subscribe message.
  /// All parameters (forward, priority, filters) are omitted,
  /// meaning the receiver will use protocol default behaviors.
  pub fn new(request_id: u64, track_namespace: Tuple, track_name: TupleField) -> Self {
    Self {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters: Vec::new(),
    }
  }

  /// Creates a Subscribe message with custom parameters explicitly provided.
  pub fn new_with_params(
    request_id: u64,
    track_namespace: Tuple,
    track_name: TupleField,
    subscribe_parameters: Vec<KeyValuePair>,
  ) -> Self {
    Self {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    }
  }
  pub fn new_next_group_start(
    request_id: u64,
    track_namespace: Tuple,
    track_name: TupleField,
    subscriber_priority: u8,
    group_order: GroupOrder,
    forward: bool,
    mut subscribe_parameters: Vec<KeyValuePair>,
  ) -> Self {
    subscribe_parameters.push(
      KeyValuePair::try_new_varint(PARAM_SUBSCRIBER_PRIORITY, subscriber_priority as u64).unwrap(),
    );
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_GROUP_ORDER, group_order as u8 as u64).unwrap());
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, if forward { 1 } else { 0 }).unwrap());

    let filter_bytes = build_filter_bytes(FilterType::NextGroupStart, None, None).unwrap();
    subscribe_parameters
      .push(KeyValuePair::try_new_bytes(PARAM_SUBSCRIPTION_FILTER, filter_bytes).unwrap());
    Self {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    }
  }

  pub fn new_latest_object(
    request_id: u64,
    track_namespace: Tuple,
    track_name: TupleField,
    subscriber_priority: u8,
    group_order: GroupOrder,
    forward: bool,
    mut subscribe_parameters: Vec<KeyValuePair>,
  ) -> Self {
    subscribe_parameters.push(
      KeyValuePair::try_new_varint(PARAM_SUBSCRIBER_PRIORITY, subscriber_priority as u64).unwrap(),
    );
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_GROUP_ORDER, group_order as u8 as u64).unwrap());
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, if forward { 1 } else { 0 }).unwrap());

    let filter_bytes = build_filter_bytes(FilterType::LatestObject, None, None).unwrap();
    subscribe_parameters
      .push(KeyValuePair::try_new_bytes(PARAM_SUBSCRIPTION_FILTER, filter_bytes).unwrap());

    Self {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    }
  }

  pub fn new_absolute_start(
    request_id: u64,
    track_namespace: Tuple,
    track_name: TupleField,
    subscriber_priority: u8,
    group_order: GroupOrder,
    forward: bool,
    start_location: Location,
    mut subscribe_parameters: Vec<KeyValuePair>,
  ) -> Self {
    subscribe_parameters.push(
      KeyValuePair::try_new_varint(PARAM_SUBSCRIBER_PRIORITY, subscriber_priority as u64).unwrap(),
    );
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_GROUP_ORDER, group_order as u8 as u64).unwrap());
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, if forward { 1 } else { 0 }).unwrap());

    let filter_bytes =
      build_filter_bytes(FilterType::AbsoluteStart, Some(start_location), None).unwrap();
    subscribe_parameters
      .push(KeyValuePair::try_new_bytes(PARAM_SUBSCRIPTION_FILTER, filter_bytes).unwrap());

    Self {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    }
  }

  pub fn new_absolute_range(
    request_id: u64,
    track_namespace: Tuple,
    track_name: TupleField,
    subscriber_priority: u8,
    group_order: GroupOrder,
    forward: bool,
    start_location: Location,
    end_group: u64,
    mut subscribe_parameters: Vec<KeyValuePair>,
  ) -> Self {
    assert!(
      end_group >= start_location.group,
      "End Group must be >= Start Group"
    );
    subscribe_parameters.push(
      KeyValuePair::try_new_varint(PARAM_SUBSCRIBER_PRIORITY, subscriber_priority as u64).unwrap(),
    );
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_GROUP_ORDER, group_order as u8 as u64).unwrap());
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, if forward { 1 } else { 0 }).unwrap());

    let filter_bytes = build_filter_bytes(
      FilterType::AbsoluteRange,
      Some(start_location),
      Some(end_group),
    )
    .unwrap();
    subscribe_parameters
      .push(KeyValuePair::try_new_bytes(PARAM_SUBSCRIPTION_FILTER, filter_bytes).unwrap());

    Self {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    }
  }

  pub fn get_full_track_name(&self) -> FullTrackName {
    FullTrackName {
      namespace: self.track_namespace.clone(),
      name: self.track_name.clone(),
    }
  }
  /// Checks if the forward parameter is set. Defaults to false (0) per Draft-16.
  pub fn should_forward(&self) -> bool {
    self
      .subscribe_parameters
      .iter()
      .find(|p| p.get_type() == PARAM_FORWARD)
      .map(|p| match p {
        KeyValuePair::VarInt { value, .. } => *value == 1,
        _ => false,
      })
      .unwrap_or(false)
  }

  /// Sets the forward parameter, overwriting any existing one.
  pub fn set_forward(&mut self, forward: bool) {
    self
      .subscribe_parameters
      .retain(|p| p.get_type() != PARAM_FORWARD);

    let val = if forward { 1 } else { 0 };
    self
      .subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, val).unwrap());
  }
  /// Gets the subscriber priority. Defaults to 0 if absent.
  pub fn subscriber_priority(&self) -> u8 {
    self
      .subscribe_parameters
      .iter()
      .find(|p| p.get_type() == PARAM_SUBSCRIBER_PRIORITY)
      .map(|p| match p {
        KeyValuePair::VarInt { value, .. } => *value as u8,
        _ => 0,
      })
      .unwrap_or(0)
  }

  /// Gets the group order. Defaults to GroupOrder::Original (or whatever your 0-value is) if absent.
  pub fn group_order(&self) -> GroupOrder {
    self
      .subscribe_parameters
      .iter()
      .find(|p| p.get_type() == PARAM_GROUP_ORDER)
      .map(|p| match p {
        KeyValuePair::VarInt { value, .. } => {
          // Fall back to Original if the byte fails to map to a valid GroupOrder enum
          GroupOrder::try_from(*value as u8).unwrap_or(GroupOrder::Original)
        }
        _ => GroupOrder::Original,
      })
      .unwrap_or(GroupOrder::Original)
  }

  /// Private helper to safely deserialize the 0x21 SUBSCRIPTION_FILTER byte payload.
  fn parse_filter(&self) -> (FilterType, Option<Location>, Option<u64>) {
    let filter_param = self
      .subscribe_parameters
      .iter()
      .find(|p| p.get_type() == PARAM_SUBSCRIPTION_FILTER);

    if let Some(KeyValuePair::Bytes { value, .. }) = filter_param {
      let mut payload = value.clone();

      // Attempt to read the filter type varint
      if let Ok(filter_type_raw) = payload.get_vi()
        && let Ok(filter_type) = FilterType::try_from(filter_type_raw)
      {
        let mut start_location = None;
        let mut end_group = None;

        // Attempt to read location/group data based on the filter type
        match filter_type {
          FilterType::AbsoluteStart => {
            if let Ok(loc) = Location::deserialize(&mut payload) {
              start_location = Some(loc);
            }
          }
          FilterType::AbsoluteRange => {
            if let Ok(loc) = Location::deserialize(&mut payload) {
              start_location = Some(loc);
              if let Ok(eg) = payload.get_vi() {
                end_group = Some(eg);
              }
            }
          }
          _ => {}
        }
        return (filter_type, start_location, end_group);
      }
    }

    // Default fallback if the parameter is missing or malformed
    (FilterType::LatestObject, None, None)
  }

  pub fn filter_type(&self) -> FilterType {
    self.parse_filter().0
  }

  pub fn start_location(&self) -> Option<Location> {
    self.parse_filter().1
  }

  pub fn end_group(&self) -> Option<u64> {
    self.parse_filter().2
  }
}
impl ControlMessageTrait for Subscribe {
  fn serialize(&self) -> Result<Bytes, ParseError> {
    let mut buf = BytesMut::new();
    buf.put_vi(ControlMessageType::Subscribe)?;

    let mut payload = BytesMut::new();
    payload.put_vi(self.request_id)?;

    payload.extend_from_slice(&self.track_namespace.serialize()?);
    payload.put_vi(self.track_name.len())?;
    payload.extend_from_slice(self.track_name.as_bytes());

    payload.put_vi(self.subscribe_parameters.len())?;
    for param in &self.subscribe_parameters {
      payload.extend_from_slice(&param.serialize()?);
    }

    let payload_len: u16 = payload
      .len()
      .try_into()
      .map_err(|e: std::num::TryFromIntError| ParseError::CastingError {
        context: "Subscribe::serialize",
        from_type: "usize",
        to_type: "u16",
        details: e.to_string(),
      })?;

    buf.put_u16(payload_len);
    buf.extend_from_slice(&payload);
    Ok(buf.freeze())
  }

  fn parse_payload(payload: &mut Bytes) -> Result<Box<Self>, ParseError> {
    let request_id = payload.get_vi()?;
    let track_namespace = Tuple::deserialize(payload)?;

    let name_len_u64 = payload.get_vi()?;
    let name_len: usize = name_len_u64
      .try_into()
      .map_err(|e: std::num::TryFromIntError| ParseError::CastingError {
        context: "Subscribe::parse_payload(track_name_len)",
        from_type: "u64",
        to_type: "usize",
        details: e.to_string(),
      })?;

    if payload.remaining() < name_len {
      return Err(ParseError::NotEnoughBytes {
        context: "Subscribe::parse_payload(track_name)",
        needed: name_len,
        available: payload.remaining(),
      });
    }
    let track_name = TupleField::new(payload.copy_to_bytes(name_len));

    let param_count_u64 = payload.get_vi()?;
    let param_count: usize =
      param_count_u64
        .try_into()
        .map_err(|e: std::num::TryFromIntError| ParseError::CastingError {
          context: "Subscribe::deserialize(param_count)",
          from_type: "u64",
          to_type: "usize",
          details: e.to_string(),
        })?;

    let mut subscribe_parameters = Vec::with_capacity(param_count);
    for _ in 0..param_count {
      let param = KeyValuePair::deserialize(payload)?;
      subscribe_parameters.push(param);
    }

    Ok(Box::new(Subscribe {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    }))
  }
  fn get_type(&self) -> ControlMessageType {
    ControlMessageType::Subscribe
  }
}
#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Buf;

  #[test]
  fn test_roundtrip() {
    let request_id = 128242;
    let track_namespace = Tuple::from_utf8_path("nein/nein/nein");
    let track_name = TupleField::from_utf8("${Name}");
    let subscriber_priority = 31;
    let group_order = GroupOrder::Original;
    let forward = false;
    let start_location = Location {
      group: 81,
      object: 81,
    };
    let end_group = 25;

    let mut subscribe_parameters = vec![
      KeyValuePair::try_new_varint(0, 10).unwrap(),
      KeyValuePair::try_new_bytes(1, Bytes::from_static(b"I'll sync you up")).unwrap(),
    ];

    // Simulating what the constructor would do:
    subscribe_parameters.push(
      KeyValuePair::try_new_varint(PARAM_SUBSCRIBER_PRIORITY, subscriber_priority as u64).unwrap(),
    );
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_GROUP_ORDER, group_order as u8 as u64).unwrap());
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, if forward { 1 } else { 0 }).unwrap());
    let filter_bytes = build_filter_bytes(
      FilterType::AbsoluteRange,
      Some(start_location.clone()),
      Some(end_group),
    )
    .unwrap();
    subscribe_parameters
      .push(KeyValuePair::try_new_bytes(PARAM_SUBSCRIPTION_FILTER, filter_bytes).unwrap());

    let subscribe = Subscribe {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    };

    let mut buf = subscribe.serialize().unwrap();
    let msg_type = buf.get_vi().unwrap();
    assert_eq!(msg_type, ControlMessageType::Subscribe as u64);
    let msg_length = buf.get_u16();
    assert_eq!(msg_length as usize, buf.remaining());
    let deserialized = Subscribe::parse_payload(&mut buf).unwrap();
    assert_eq!(*deserialized, subscribe);
    assert!(!buf.has_remaining());
  }

  #[test]
  fn test_excess_roundtrip() {
    let request_id = 128242;
    let track_namespace = Tuple::from_utf8_path("nein/nein/nein");
    let track_name = TupleField::from_utf8("${Name}");
    let subscriber_priority = 31;
    let group_order = GroupOrder::Original;
    let forward = true;
    let start_location = Location {
      group: 81,
      object: 81,
    };
    let end_group = 25;

    let mut subscribe_parameters = vec![
      KeyValuePair::try_new_varint(0, 10).unwrap(),
      KeyValuePair::try_new_bytes(1, Bytes::from_static(b"I'll sync you up")).unwrap(),
    ];

    subscribe_parameters.push(
      KeyValuePair::try_new_varint(PARAM_SUBSCRIBER_PRIORITY, subscriber_priority as u64).unwrap(),
    );
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_GROUP_ORDER, group_order as u8 as u64).unwrap());
    subscribe_parameters
      .push(KeyValuePair::try_new_varint(PARAM_FORWARD, if forward { 1 } else { 0 }).unwrap());
    let filter_bytes = build_filter_bytes(
      FilterType::AbsoluteRange,
      Some(start_location.clone()),
      Some(end_group),
    )
    .unwrap();
    subscribe_parameters
      .push(KeyValuePair::try_new_bytes(PARAM_SUBSCRIPTION_FILTER, filter_bytes).unwrap());

    let subscribe = Subscribe {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters,
    };

    let serialized = subscribe.serialize().unwrap();
    let mut excess = BytesMut::new();
    excess.extend_from_slice(&serialized);
    excess.extend_from_slice(&[9u8, 1u8, 1u8]);
    let mut buf = excess.freeze();

    let msg_type = buf.get_vi().unwrap();
    assert_eq!(msg_type, ControlMessageType::Subscribe as u64);
    let msg_length = buf.get_u16();

    assert_eq!(msg_length as usize, buf.remaining() - 3);
    let deserialized = Subscribe::parse_payload(&mut buf).unwrap();
    assert_eq!(*deserialized, subscribe);
    assert_eq!(buf.chunk(), &[9u8, 1u8, 1u8]);
  }

  #[test]
  fn test_partial_message() {
    let request_id = 128242;
    let track_namespace = Tuple::from_utf8_path("nein/nein/nein");
    let track_name = TupleField::from_utf8("${Name}");

    let subscribe = Subscribe {
      request_id,
      track_namespace,
      track_name,
      subscribe_parameters: vec![],
    };

    let mut buf = subscribe.serialize().unwrap();
    let msg_type = buf.get_vi().unwrap();
    assert_eq!(msg_type, ControlMessageType::Subscribe as u64);
    let msg_length = buf.get_u16();
    assert_eq!(msg_length as usize, buf.remaining());

    let upper = buf.remaining() / 2;
    let mut partial = buf.slice(..upper);
    let deserialized = Subscribe::parse_payload(&mut partial);
    assert!(deserialized.is_err());
  }
}
