import { describe, expect, it, vi } from 'vitest'
import {
  createEnvironmentFromPairingOffer,
  getPreferredPairingOffer,
  type KnownRuntimeEnvironment
} from '../../shared/runtime-environments'

type BridgeDouble = { target: { directEndpoint: string | null }; disposed: boolean }

const { bridges } = vi.hoisted(() => {
  const created: BridgeDouble[] = []
  return { bridges: created }
})

vi.mock('./runtime-environment-relay-bridge', () => ({
  RuntimeEnvironmentRelayBridge: class {
    readonly publicKeyB64 = 'YnJpZGdlLWtleS1ieXRlcy1mb3ItdGVzdC0zMi1ieXRlcw=='
    readonly endpoint: string
    disposed = false
    constructor(readonly target: { environmentId: string; directEndpoint: string | null }) {
      this.endpoint = `ws://relay.orca.invalid/${target.environmentId}`
      bridges.push(this)
    }
    dispose(): void {
      this.disposed = true
    }
  }
}))

const { getRuntimeEnvironmentConnectPairing } = await import('./runtime-environment-relay-route')

const relay = {
  endpoint: {
    v: 1 as const,
    directorUrl: 'https://relay.onorca.dev',
    cellUrl: 'https://relay-c1.onorca.dev',
    assignmentEpoch: 1,
    relayHostId: 'AbCdEf0123_-xyZ9',
    e2eeFraming: 2 as const
  },
  credential: { token: 'T'.repeat(43), version: 1, expiresAt: 1_000 }
}

function environment(
  id: string,
  endpoint: string,
  withRelay: boolean,
  now = 100
): KnownRuntimeEnvironment {
  return createEnvironmentFromPairingOffer({
    id,
    name: id,
    now,
    offer: {
      v: 2,
      endpoint,
      deviceToken: 'device-token',
      publicKeyB64: Buffer.from(new Uint8Array(32).fill(5)).toString('base64')
    },
    ...(withRelay ? { relay } : {})
  })
}

describe('getRuntimeEnvironmentConnectPairing', () => {
  it('leaves environments without a Relay route on their paired endpoint', () => {
    const direct = environment('direct-only', 'ws://100.64.1.20:6768', false)

    expect(getRuntimeEnvironmentConnectPairing('/tmp/orca', direct)).toEqual(
      getPreferredPairingOffer(direct)
    )
    expect(bridges).toHaveLength(0)
  })

  it('routes Relay environments through their bridge and skips an unreachable loopback endpoint', () => {
    const pairing = getRuntimeEnvironmentConnectPairing(
      '/tmp/orca',
      environment('relay-loopback', 'ws://127.0.0.1:6768', true)
    )

    expect(pairing).toMatchObject({
      endpoint: 'ws://relay.orca.invalid/relay-loopback',
      deviceToken: 'device-token',
      publicKeyB64: 'YnJpZGdlLWtleS1ieXRlcy1mb3ItdGVzdC0zMi1ieXRlcw=='
    })
    expect(bridges.at(-1)?.target.directEndpoint).toBeNull()
  })

  it('keeps a routable paired endpoint as the first route and reuses one bridge per pairing', () => {
    const tailnet = environment('relay-tailnet', 'ws://100.64.1.20:6768', true)
    getRuntimeEnvironmentConnectPairing('/tmp/orca', tailnet)
    getRuntimeEnvironmentConnectPairing('/tmp/orca', tailnet)
    const first = bridges.at(-1)

    expect(first?.target.directEndpoint).toBe('ws://100.64.1.20:6768')
    expect(bridges.filter((bridge) => bridge === first)).toHaveLength(1)

    getRuntimeEnvironmentConnectPairing('/tmp/orca', { ...tailnet, pairingRevision: 200 })
    expect(first?.disposed).toBe(true)
    expect(bridges.at(-1)).not.toBe(first)
  })
})
