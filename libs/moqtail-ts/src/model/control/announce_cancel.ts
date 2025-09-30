/**
 * Copyright 2025 The MOQtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { BaseByteBuffer, ByteBuffer, FrozenByteBuffer } from '../common/byte_buffer'
import { Tuple } from '../common/tuple'
import { ReasonPhrase } from '../common/reason_phrase'
import { ControlMessageType, AnnounceErrorCode, announceErrorCodeFromBigInt } from './constant'
import { LengthExceedsMaxError } from '../error/error'

export class AnnounceCancel {
  constructor(
    public readonly trackNamespace: Tuple,
    public readonly errorCode: AnnounceErrorCode,
    public readonly reasonPhrase: ReasonPhrase,
  ) {}

  getType(): ControlMessageType {
    return ControlMessageType.AnnounceCancel
  }

  serialize(): FrozenByteBuffer {
    const buf = new ByteBuffer()
    buf.putVI(ControlMessageType.AnnounceCancel)
    const payload = new ByteBuffer()
    payload.putTuple(this.trackNamespace)
    payload.putVI(this.errorCode)
    payload.putReasonPhrase(this.reasonPhrase)
    const payloadBytes = payload.toUint8Array()
    if (payloadBytes.length > 0xffff) {
      throw new LengthExceedsMaxError('AnnounceCancel::serialize(payloadBytes.length)', 0xffff, payloadBytes.length)
    }
    buf.putU16(payloadBytes.length)
    buf.putBytes(payloadBytes)
    return buf.freeze()
  }

  static parsePayload(buf: BaseByteBuffer): AnnounceCancel {
    const trackNamespace = buf.getTuple()
    const errorCodeRaw = buf.getVI()
    const errorCode = announceErrorCodeFromBigInt(errorCodeRaw)
    const reasonPhrase = buf.getReasonPhrase()
    return new AnnounceCancel(trackNamespace, errorCode, reasonPhrase)
  }
}

if (import.meta.vitest) {
  const { describe, test, expect } = import.meta.vitest
  describe('AnnounceCancel', () => {
    test('roundtrip', () => {
      const errorCode = AnnounceErrorCode.ExpiredAuthToken
      const reasonPhrase = new ReasonPhrase('why are you running?')
      const trackNamespace = Tuple.fromUtf8Path('valid/track/namespace')
      const msg = new AnnounceCancel(trackNamespace, errorCode, reasonPhrase)
      const frozen = msg.serialize()
      const msgType = frozen.getVI()
      expect(msgType).toBe(BigInt(ControlMessageType.AnnounceCancel))
      const msgLength = frozen.getU16()
      expect(msgLength).toBe(frozen.remaining)
      const deserialized = AnnounceCancel.parsePayload(frozen)
      expect(deserialized.trackNamespace.equals(msg.trackNamespace)).toBe(true)
      expect(deserialized.errorCode).toBe(msg.errorCode)
      expect(deserialized.reasonPhrase.phrase).toBe(msg.reasonPhrase.phrase)
      expect(frozen.remaining).toBe(0)
    })
    test('excess roundtrip', () => {
      const errorCode = AnnounceErrorCode.InternalError
      const reasonPhrase = new ReasonPhrase('bomboclad')
      const trackNamespace = Tuple.fromUtf8Path('another/valid/track/namespace')
      const msg = new AnnounceCancel(trackNamespace, errorCode, reasonPhrase)
      const serialized = msg.serialize().toUint8Array()
      const excess = new Uint8Array([9, 1, 1])
      const buf = new ByteBuffer()
      buf.putBytes(serialized)
      buf.putBytes(excess)
      const frozen = buf.freeze()
      const msgType = frozen.getVI()
      expect(msgType).toBe(BigInt(ControlMessageType.AnnounceCancel))
      const msgLength = frozen.getU16()
      expect(msgLength).toBe(frozen.remaining - 3)
      const deserialized = AnnounceCancel.parsePayload(frozen)
      expect(deserialized.trackNamespace.equals(msg.trackNamespace)).toBe(true)
      expect(deserialized.errorCode).toBe(msg.errorCode)
      expect(deserialized.reasonPhrase.phrase).toBe(msg.reasonPhrase.phrase)
      expect(frozen.remaining).toBe(3)
      expect(Array.from(frozen.getBytes(3))).toEqual([9, 1, 1])
    })
    test('partial message', () => {
      const errorCode = AnnounceErrorCode.MalformedAuthToken
      const reasonPhrase = new ReasonPhrase('Uvuvwevwevwe')
      const trackNamespace = Tuple.fromUtf8Path('Onyetenyevwe/Ugwemuhwem/Osas')
      const msg = new AnnounceCancel(trackNamespace, errorCode, reasonPhrase)
      const serialized = msg.serialize().toUint8Array()
      const upper = Math.floor(serialized.length / 2)
      const partial = serialized.slice(0, upper)
      const frozen = new FrozenByteBuffer(partial)
      expect(() => {
        frozen.getVI()
        frozen.getU16()
        AnnounceCancel.parsePayload(frozen)
      }).toThrow()
    })
  })
}
