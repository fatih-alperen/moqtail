/**
 * Copyright 2025 The MOQtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { BaseByteBuffer, ByteBuffer, FrozenByteBuffer } from '../common/byte_buffer'
import { Location } from '../common/location'
import { KeyValuePair, isVarInt, isBytes } from '../common/pair'
import { ControlMessageType, FilterType, GroupOrder } from '../control/constant'
import { FullTrackName } from '../data'

export const PARAM_FORWARD = 0x10n
export const PARAM_SUBSCRIBER_PRIORITY = 0x20n
export const PARAM_SUBSCRIPTION_FILTER = 0x21n
export const PARAM_GROUP_ORDER = 0x22n

/**
 * Helper to build the byte payload for the SUBSCRIPTION_FILTER parameter
 */
function buildFilterBytes(filterType: FilterType, startLocation?: Location, endGroup?: bigint): Uint8Array {
  const buf = new ByteBuffer()
  buf.putVI(filterType)

  if (filterType === FilterType.AbsoluteStart) {
    if (!startLocation) throw new Error('StartLocation required for AbsoluteStart')
    buf.putLocation(startLocation)
  } else if (filterType === FilterType.AbsoluteRange) {
    if (!startLocation) throw new Error('StartLocation required for AbsoluteRange')
    if (endGroup == null) throw new Error('EndGroup required for AbsoluteRange')
    buf.putLocation(startLocation)
    buf.putVI(endGroup)
  }

  return buf.toUint8Array()
}

export class Subscribe {
  private constructor(
    public requestId: bigint,
    public fullTrackName: FullTrackName,
    public parameters: KeyValuePair[],
  ) {}

  // --- GETTERS & SETTERS ---

  shouldForward(): boolean {
    const p = this.parameters.find((p) => p.typeValue === PARAM_FORWARD)
    if (p && isVarInt(p)) {
      return p.value === 1n
    }
    return false
  }

  setForward(forward: boolean): void {
    this.parameters = this.parameters.filter((p) => p.typeValue !== PARAM_FORWARD)
    this.parameters.push(KeyValuePair.tryNewVarInt(PARAM_FORWARD, forward ? 1n : 0n))
  }

  getSubscriberPriority(): number {
    const p = this.parameters.find((p) => p.typeValue === PARAM_SUBSCRIBER_PRIORITY)
    if (p && isVarInt(p)) {
      return Number(p.value)
    }
    return 0
  }

  getGroupOrder(): GroupOrder {
    const p = this.parameters.find((p) => p.typeValue === PARAM_GROUP_ORDER)
    if (p && isVarInt(p)) {
      return Number(p.value) as GroupOrder
    }
    return GroupOrder.Original
  }

  private parseFilter(): {
    filterType: FilterType
    startLocation?: Location | undefined
    endGroup?: bigint | undefined
  } {
    const p = this.parameters.find((p) => p.typeValue === PARAM_SUBSCRIPTION_FILTER)
    if (p && isBytes(p)) {
      try {
        const buf = new ByteBuffer()
        buf.putBytes(p.value)
        const filterTypeRaw = Number(buf.getVI()) as FilterType

        let startLocation: Location | undefined = undefined
        let endGroup: bigint | undefined = undefined

        if (filterTypeRaw === FilterType.AbsoluteStart || filterTypeRaw === FilterType.AbsoluteRange) {
          startLocation = buf.getLocation()
        }
        if (filterTypeRaw === FilterType.AbsoluteRange) {
          endGroup = buf.getVI()
        }

        return { filterType: filterTypeRaw, startLocation, endGroup }
      } catch (e) {}
    }
    return { filterType: FilterType.LatestObject }
  }

  getFilterType(): FilterType {
    return this.parseFilter().filterType
  }

  getStartLocation(): Location | undefined {
    return this.parseFilter().startLocation
  }

  getEndGroup(): bigint | undefined {
    return this.parseFilter().endGroup
  }

  static newBasic(requestId: bigint, fullTrackName: FullTrackName): Subscribe {
    return new Subscribe(requestId, fullTrackName, [])
  }

  static newWithParams(requestId: bigint, fullTrackName: FullTrackName, parameters: KeyValuePair[]): Subscribe {
    return new Subscribe(requestId, fullTrackName, parameters)
  }

  static newNextGroupStart(
    requestId: bigint,
    fullTrackName: FullTrackName,
    subscriberPriority: number,
    groupOrder: GroupOrder,
    forward: boolean,
    parameters: KeyValuePair[],
  ): Subscribe {
    const params = [...parameters]
    params.push(KeyValuePair.tryNewVarInt(PARAM_SUBSCRIBER_PRIORITY, BigInt(subscriberPriority)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_GROUP_ORDER, BigInt(groupOrder)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_FORWARD, forward ? 1n : 0n))
    params.push(KeyValuePair.tryNewBytes(PARAM_SUBSCRIPTION_FILTER, buildFilterBytes(FilterType.NextGroupStart)))

    return new Subscribe(requestId, fullTrackName, params)
  }

  static newLatestObject(
    requestId: bigint,
    fullTrackName: FullTrackName,
    subscriberPriority: number,
    groupOrder: GroupOrder,
    forward: boolean,
    parameters: KeyValuePair[],
  ): Subscribe {
    const params = [...parameters]
    params.push(KeyValuePair.tryNewVarInt(PARAM_SUBSCRIBER_PRIORITY, BigInt(subscriberPriority)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_GROUP_ORDER, BigInt(groupOrder)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_FORWARD, forward ? 1n : 0n))
    params.push(KeyValuePair.tryNewBytes(PARAM_SUBSCRIPTION_FILTER, buildFilterBytes(FilterType.LatestObject)))

    return new Subscribe(requestId, fullTrackName, params)
  }

  static newAbsoluteStart(
    requestId: bigint,
    fullTrackName: FullTrackName,
    subscriberPriority: number,
    groupOrder: GroupOrder,
    forward: boolean,
    startLocation: Location,
    parameters: KeyValuePair[],
  ): Subscribe {
    const params = [...parameters]
    params.push(KeyValuePair.tryNewVarInt(PARAM_SUBSCRIBER_PRIORITY, BigInt(subscriberPriority)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_GROUP_ORDER, BigInt(groupOrder)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_FORWARD, forward ? 1n : 0n))
    params.push(
      KeyValuePair.tryNewBytes(PARAM_SUBSCRIPTION_FILTER, buildFilterBytes(FilterType.AbsoluteStart, startLocation)),
    )

    return new Subscribe(requestId, fullTrackName, params)
  }

  static newAbsoluteRange(
    requestId: bigint,
    fullTrackName: FullTrackName,
    subscriberPriority: number,
    groupOrder: GroupOrder,
    forward: boolean,
    startLocation: Location,
    endGroup: bigint,
    parameters: KeyValuePair[],
  ): Subscribe {
    if (endGroup < startLocation.group) {
      throw new Error('End Group must be >= Start Group')
    }
    const params = [...parameters]
    params.push(KeyValuePair.tryNewVarInt(PARAM_SUBSCRIBER_PRIORITY, BigInt(subscriberPriority)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_GROUP_ORDER, BigInt(groupOrder)))
    params.push(KeyValuePair.tryNewVarInt(PARAM_FORWARD, forward ? 1n : 0n))
    params.push(
      KeyValuePair.tryNewBytes(
        PARAM_SUBSCRIPTION_FILTER,
        buildFilterBytes(FilterType.AbsoluteRange, startLocation, endGroup),
      ),
    )

    return new Subscribe(requestId, fullTrackName, params)
  }

  // --- SERIALIZATION ---

  serialize(): FrozenByteBuffer {
    const buf = new ByteBuffer()
    buf.putVI(ControlMessageType.Subscribe)

    const payload = new ByteBuffer()
    payload.putVI(this.requestId)
    payload.putBytes(this.fullTrackName.serialize().toUint8Array())

    payload.putVI(this.parameters.length)
    for (const param of this.parameters) {
      payload.putBytes(param.serialize().toUint8Array())
    }

    const payloadBytes = payload.toUint8Array()
    buf.putU16(payloadBytes.length)
    buf.putBytes(payloadBytes)

    return buf.freeze()
  }

  static parsePayload(buf: BaseByteBuffer): Subscribe {
    const requestId = buf.getVI()
    const fullTrackName = buf.getFullTrackName()

    const paramCount = Number(buf.getVI())
    const parameters: KeyValuePair[] = []
    for (let i = 0; i < paramCount; i++) {
      parameters.push(KeyValuePair.deserialize(buf))
    }

    return new Subscribe(requestId, fullTrackName, parameters)
  }
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  function buildTestSubscribe(): Subscribe {
    return Subscribe.newAbsoluteRange(
      128242n,
      FullTrackName.tryNew('track/namespace', 'trackName'),
      31,
      GroupOrder.Original,
      true,
      new Location(81n, 81n),
      100n,
      [KeyValuePair.tryNewVarInt(0n, 10n), KeyValuePair.tryNewBytes(1n, new TextEncoder().encode('DemoString'))],
    )
  }

  describe('Subscribe', () => {
    it('should roundtrip correctly', () => {
      const subscribe = buildTestSubscribe()
      const serialized = subscribe.serialize()

      const buf = new ByteBuffer()
      buf.putBytes(serialized.toUint8Array())
      const msgType = buf.getVI()
      expect(msgType).toBe(BigInt(ControlMessageType.Subscribe))

      const msgLength = buf.getU16()
      expect(msgLength).toBe(buf.remaining)

      const deserialized = Subscribe.parsePayload(buf)
      expect(deserialized).toEqual(subscribe)
      expect(buf.remaining).toBe(0)
    })

    it('should roundtrip with excess trailing bytes', () => {
      const subscribe = buildTestSubscribe()
      const serialized = subscribe.serialize()
      const extra = new Uint8Array([...serialized.toUint8Array(), 9, 1, 1])

      const buf = new ByteBuffer()
      buf.putBytes(extra)

      const msgType = buf.getVI()
      expect(msgType).toBe(BigInt(ControlMessageType.Subscribe))

      const msgLength = buf.getU16()
      expect(msgLength).toBe(buf.remaining - 3)

      const deserialized = Subscribe.parsePayload(buf)
      expect(deserialized).toEqual(subscribe)

      const trailing = buf.toUint8Array().slice(buf.offset)
      expect(Array.from(trailing)).toEqual([9, 1, 1])
    })

    describe('Subscribe Constructors', () => {
      it('should create a Subscribe with AbsoluteRange filter', () => {
        const subscribe = Subscribe.newAbsoluteRange(
          128242n,
          FullTrackName.tryNew('track/namespace', 'trackName'),
          31,
          GroupOrder.Original,
          true,
          new Location(81n, 81n),
          100n,
          [],
        )

        // Strict verification of parameter extraction mapping
        expect(subscribe.getSubscriberPriority()).toBe(31)
        expect(subscribe.getGroupOrder()).toBe(GroupOrder.Original)
        expect(subscribe.shouldForward()).toBe(true)
        expect(subscribe.getFilterType()).toBe(FilterType.AbsoluteRange)
        expect(subscribe.getStartLocation()).toEqual(new Location(81n, 81n))
        expect(subscribe.getEndGroup()).toBe(100n)
      })

      it('should throw an error if EndGroup < StartGroup', () => {
        expect(() =>
          Subscribe.newAbsoluteRange(
            128242n,
            FullTrackName.tryNew('track/namespace', 'trackName'),
            31,
            GroupOrder.Original,
            true,
            new Location(81n, 81n),
            80n,
            [],
          ),
        ).toThrow('End Group must be >= Start Group')
      })
    })

    it('should handle empty parameters using newBasic and return defaults', () => {
      const subscribe = Subscribe.newBasic(128242n, FullTrackName.tryNew('track/namespace', 'trackName'))

      expect(subscribe.shouldForward()).toBe(false)
      expect(subscribe.getSubscriberPriority()).toBe(0)

      const serialized = subscribe.serialize()
      const buf = new ByteBuffer()
      buf.putBytes(serialized.toUint8Array())

      const msgType = buf.getVI()
      expect(msgType).toBe(BigInt(ControlMessageType.Subscribe))
      buf.getU16()

      const deserialized = Subscribe.parsePayload(buf)
      expect(deserialized).toEqual(subscribe)
      expect(buf.remaining).toBe(0)
    })

    it('should throw on partial message', () => {
      const subscribe = buildTestSubscribe()
      const serialized = subscribe.serialize()
      const serializedBytes = serialized.toUint8Array()

      const partialBytes = serializedBytes.slice(0, Math.floor(serializedBytes.length / 2))
      const buf = new ByteBuffer()
      buf.putBytes(partialBytes)

      try {
        buf.getVI()
        buf.getU16()

        expect(() => Subscribe.parsePayload(buf)).toThrow()
      } catch (err) {
        expect(err).toBeInstanceOf(Error)
      }
    })
  })
}
