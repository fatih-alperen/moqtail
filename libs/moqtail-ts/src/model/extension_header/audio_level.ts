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

import { KeyValuePair } from '../common/pair'
import { LOCHeaderExtensionId } from './constant'

export class AudioLevel {
  static readonly TYPE = LOCHeaderExtensionId.AudioLevel
  constructor(public readonly audioLevel: bigint) {}

  toKeyValuePair(): KeyValuePair {
    return KeyValuePair.tryNewVarInt(AudioLevel.TYPE, this.audioLevel)
  }

  static fromKeyValuePair(pair: KeyValuePair): AudioLevel | undefined {
    const type = Number(pair.typeValue)
    if (type === AudioLevel.TYPE && typeof pair.value === 'bigint') {
      return new AudioLevel(pair.value)
    }
    return undefined
  }
}

if (import.meta.vitest) {
  const { describe, test, expect } = import.meta.vitest
  describe('AudioLevelExtensionHeader', () => {
    test('should roundtrip AudioLevel', () => {
      const value = 42n
      const pair = new AudioLevel(value).toKeyValuePair()
      const header = AudioLevel.fromKeyValuePair(pair)
      expect(header).toBeInstanceOf(AudioLevel)
      expect(header?.audioLevel).toBe(value)
    })
  })
}
