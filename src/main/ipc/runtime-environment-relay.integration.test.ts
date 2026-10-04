import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { WebSocketServer, type RawData, type WebSocket } from 'ws'
import type { DeviceCredentialInstalled } from '../../shared/mobile-relay-credential-contract'
import { parsePairingCode, type PairingOffer } from '../../shared/pairing'
import { ELECTRON_REMOTE_RUNTIME_CLIENT_CAPABILITIES } from '../../shared/electron-remote-runtime-client-capabilities'
import { sendRemoteRuntimeRequest } from '../../shared/remote-runtime-client'
import { publicKeyFromBase64 } from '../../shared/e2ee-crypto'
import type { RuntimeEnvironmentRelayRoute } from '../../shared/runtime-environments'
import type { RuntimeStatus } from '../../shared/runtime-types'
import { OrcaRuntimeService } from '../runtime/orca-runtime'
import { OrcaRuntimeRpcServer } from '../runtime/runtime-rpc'
import { CloudRelayTransport } from '../runtime/rpc/relay-transport'
import { deriveRelayHostId } from '../runtime/relay/relay-http-client'
import { RuntimeEnvironmentRelayBridge } from './runtime-environment-relay-bridge'
import {
  hashRuntimeRelayCredential,
  pairRuntimeEnvironmentThroughRelay
} from './runtime-environment-relay-pairing'

vi.mock('../git/worktree', () => ({
  listWorktrees: vi.fn().mockResolvedValue([]),
  listWorktreesStrict: vi.fn().mockResolvedValue([])
}))

const DAY_MS = 24 * 60 * 60 * 1000
const INVITE_TOKEN = 'I'.repeat(43)
const RENEWED_EXPIRES_AT = Date.now() + 30 * DAY_MS

type Harness = Awaited<ReturnType<typeof startHarness>>

const cleanups: (() => Promise<void> | void)[] = []

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).toReversed()) {
    await cleanup()
  }
})

function forward(target: WebSocket, raw: RawData, isBinary: boolean): void {
  if (target.readyState === target.OPEN) {
    target.send(raw, { binary: isBinary })
  }
}

/** A runtime server behind a fake Relay cell that splices clients onto host data sockets. */
async function startHarness(options: { cellHonorsRevoke: boolean }) {
  const userDataPath = mkdtempSync(join(tmpdir(), 'orca-runtime-relay-'))
  cleanups.push(() => rmSync(userDataPath, { recursive: true, force: true }))
  const server = new OrcaRuntimeRpcServer({
    runtime: new OrcaRuntimeService(),
    userDataPath,
    enableWebSocket: true,
    wsPort: 0
  })
  const cell = new WebSocketServer({ host: '127.0.0.1', port: 0, perMessageDeflate: false })
  await new Promise<void>((resolve) => cell.once('listening', resolve))
  const address = cell.address()
  if (!address || typeof address === 'string') {
    throw new Error('expected a TCP address for the fake cell')
  }
  const cellUrl = `http://127.0.0.1:${address.port}`
  const connectUrl = (_baseUrl: string, hostId: string) =>
    `ws://127.0.0.1:${address.port}/v1/connect/${hostId}`
  const installs = new Map<string, DeviceCredentialInstalled>()
  const revokedDevices = new Set<string>()
  const state: { resumeHash: string | null; deviceId: string; clientConnections: number } = {
    resumeHash: null,
    deviceId: '',
    clientConnections: 0
  }

  await server.start()
  cleanups.push(() => server.stop())
  const relayHostId = deriveRelayHostId(server.getE2EEKeypair()!.publicKey)
  const relayEndpoint = {
    v: 1 as const,
    directorUrl: 'https://director.relay.test',
    cellUrl: 'https://cell.relay.test',
    assignmentEpoch: 1,
    relayHostId,
    e2eeFraming: 2 as const
  }
  server.setMobileRelayPairingProvider({
    createPairingRelay: async (relayDeviceId) => ({
      relay: {
        ...relayEndpoint,
        inviteToken: INVITE_TOKEN,
        inviteExpiresAt: Date.now() + 5 * 60_000
      },
      binding: { relayHostId, relayDeviceId, ownerIdentityKey: 'owner' }
    }),
    onDeviceRevokeQueued: (item) => revokedDevices.add(item.relayDeviceId),
    getEndpoints: async (_context, params) => {
      const installed = params.installReqId ? installs.get(params.installReqId) : undefined
      return {
        v: 1,
        relay: relayEndpoint,
        ...(params.installReqId
          ? {
              installStatus: installed
                ? { v: 1, reqId: params.installReqId, state: 'committed', result: installed }
                : { v: 1, reqId: params.installReqId, state: 'not-found' }
            }
          : {}),
        ...(params.resumeConfirmReqId
          ? {
              resumeConfirmation: {
                v: 1,
                reqId: params.resumeConfirmReqId,
                currentVersion: 2,
                acceptedAs: 'current',
                renewed: true,
                resumeExpiresAt: RENEWED_EXPIRES_AT
              }
            }
          : {})
      }
    },
    provisionRelay: async (_context, params) => {
      state.resumeHash = params.newResumeTokenHash
      const installed: DeviceCredentialInstalled = {
        v: 1,
        reqId: params.reqId,
        authorizationMode: 'relay-basis',
        currentVersion: 1,
        resumeExpiresAt: Date.now() + 30 * DAY_MS
      }
      installs.set(params.reqId, installed)
      return installed
    }
  })

  const transport = new CloudRelayTransport({ cellUrl, relayHostId, generation: 1 })
  server.getMobileSocketWiring()!.attachTransport(transport, (ws) => transport.metadataFor(ws))
  await transport.start()
  cleanups.push(() => transport.stop())

  const pendingClients = new Map<string, { client: WebSocket; kind: 'invite' | 'resume' }>()
  let connectionSequence = 0
  cell.on('connection', (socket, request) => {
    socket.once('message', (raw) => {
      const auth: unknown = JSON.parse(raw.toString())
      if (request.url?.startsWith('/v1/host/data/')) {
        const connId = decodeURIComponent(request.url.slice('/v1/host/data/'.length))
        const pending = pendingClients.get(connId)
        if (!pending) {
          socket.close(4401)
          return
        }
        pendingClients.delete(connId)
        socket.on('message', (data, isBinary) => forward(pending.client, data, isBinary))
        pending.client.on('message', (data, isBinary) => forward(socket, data, isBinary))
        socket.once('close', () => pending.client.close(4408))
        pending.client.once('close', () => socket.close())
        pending.client.send(
          JSON.stringify(
            pending.kind === 'invite'
              ? {
                  type: 'relay-hello',
                  ok: true,
                  credentialKind: 'invite',
                  leaseExpiresAt: Date.now() + 60_000
                }
              : {
                  type: 'relay-hello',
                  ok: true,
                  credentialKind: 'resume',
                  leaseExpiresAt: Date.now() + 60_000,
                  acceptedCredentialVersion: 1,
                  acceptedAs: 'current',
                  resumeExpiresAt: Date.now() + 30 * DAY_MS
                }
          )
        )
        return
      }
      const credential =
        typeof auth === 'object' && auth !== null && 'credential' in auth
          ? String(auth.credential)
          : ''
      const kind =
        credential === INVITE_TOKEN
          ? 'invite'
          : hashRuntimeRelayCredential(credential) === state.resumeHash &&
              !(options.cellHonorsRevoke && revokedDevices.has(state.deviceId))
            ? 'resume'
            : null
      if (!kind) {
        socket.send(JSON.stringify({ type: 'relay-hello', ok: false, code: 4401 }))
        socket.close(4401)
        return
      }
      state.clientConnections += 1
      const connId = `conn-${++connectionSequence}`
      pendingClients.set(connId, { client: socket, kind })
      void transport.openConnection({
        connId,
        connTicket: 'A'.repeat(43),
        kind,
        relayDeviceId: state.deviceId,
        attachDeadlineMs: 5_000
      })
    })
  })
  cleanups.push(
    () =>
      new Promise<void>((resolve) => {
        for (const client of cell.clients) {
          client.terminate()
        }
        cell.close(() => resolve())
      })
  )

  const offer = await server.createRuntimeRelayPairingOffer({ address: '127.0.0.1' })
  if (!offer.available || !offer.relay.available) {
    throw new Error('expected a runtime Relay pairing offer')
  }
  state.deviceId = offer.deviceId
  const relayOffer = parsePairingCode(offer.relay.pairingUrl)
  if (!relayOffer?.relay) {
    throw new Error('expected Relay metadata in the runtime pairing link')
  }
  return { server, state, connectUrl, offer, relayOffer, revokedDevices }
}

async function pairAndBridge(
  harness: Harness,
  overrides: { directEndpoint?: string | null; expiresAt?: number } = {}
) {
  const paired = await pairRuntimeEnvironmentThroughRelay(
    harness.relayOffer,
    harness.relayOffer.relay!,
    { connectUrl: harness.connectUrl }
  )
  if (!paired.ok) {
    throw new Error(`relay pairing failed: ${paired.message}`)
  }
  let route: RuntimeEnvironmentRelayRoute = {
    ...paired.route,
    credential: {
      ...paired.route.credential,
      expiresAt: overrides.expiresAt ?? paired.route.credential.expiresAt
    }
  }
  const bridge = new RuntimeEnvironmentRelayBridge({
    environmentId: 'env-relay',
    deviceToken: harness.relayOffer.deviceToken,
    hostPublicKey: publicKeyFromBase64(harness.relayOffer.publicKeyB64),
    directEndpoint: overrides.directEndpoint ?? null,
    readRoute: () => route,
    writeRoute: (update) => {
      route = update(route)
    },
    connectUrl: harness.connectUrl
  })
  cleanups.push(() => bridge.dispose())
  const pairing: PairingOffer = {
    v: 2,
    endpoint: bridge.endpoint,
    deviceToken: harness.relayOffer.deviceToken,
    publicKeyB64: bridge.publicKeyB64
  }
  return { paired, pairing, readRoute: () => route }
}

function statusThrough(pairing: PairingOffer) {
  return sendRemoteRuntimeRequest<RuntimeStatus>(
    pairing,
    'status.get',
    undefined,
    10_000,
    undefined,
    undefined,
    ELECTRON_REMOTE_RUNTIME_CLIENT_CAPABILITIES
  )
}

describe('runtime pairing through Orca Relay', () => {
  it('pairs from a Relay link and serves the unchanged client stack through the bridge', async () => {
    const harness = await startHarness({ cellHonorsRevoke: true })
    const { paired, pairing } = await pairAndBridge(harness)

    expect(paired.runtimeStatus).toMatchObject({ deviceScope: 'runtime' })
    expect(hashRuntimeRelayCredential(paired.route.credential.token)).toBe(harness.state.resumeHash)
    const response = await statusThrough(pairing)

    expect(response).toMatchObject({ ok: true, result: { deviceScope: 'runtime' } })
    expect(harness.state.clientConnections).toBe(2)
  }, 20_000)

  it('reports a revoked grant as unauthorized when the host refuses the device', async () => {
    const harness = await startHarness({ cellHonorsRevoke: false })
    const { pairing } = await pairAndBridge(harness)
    await expect(statusThrough(pairing)).resolves.toMatchObject({ ok: true })

    expect(harness.server.revokeRuntimeAccess(harness.offer.deviceId)).toBe(true)

    expect(harness.revokedDevices.has(harness.offer.deviceId)).toBe(true)
    await expect(statusThrough(pairing)).rejects.toMatchObject({ code: 'unauthorized' })
  }, 20_000)

  it('reports a revoked cloud credential as unauthorized', async () => {
    const harness = await startHarness({ cellHonorsRevoke: true })
    const { pairing } = await pairAndBridge(harness)

    expect(harness.server.revokeRuntimeAccess(harness.offer.deviceId)).toBe(true)

    await expect(statusThrough(pairing)).rejects.toMatchObject({ code: 'unauthorized' })
  }, 20_000)

  it('renews a resume credential that is close to expiry', async () => {
    const harness = await startHarness({ cellHonorsRevoke: true })
    const { pairing, readRoute } = await pairAndBridge(harness, {
      expiresAt: Date.now() + DAY_MS
    })

    await expect(statusThrough(pairing)).resolves.toMatchObject({ ok: true })

    expect(readRoute().credential).toMatchObject({ version: 2, expiresAt: RENEWED_EXPIRES_AT })
  }, 20_000)

  it('uses the paired endpoint when it answers and Relay only when it does not', async () => {
    const harness = await startHarness({ cellHonorsRevoke: true })
    const direct = await pairAndBridge(harness, {
      directEndpoint: harness.server.getWebSocketEndpoint()
    })
    const relayDialsAfterPairing = harness.state.clientConnections

    await expect(statusThrough(direct.pairing)).resolves.toMatchObject({ ok: true })
    expect(harness.state.clientConnections).toBe(relayDialsAfterPairing)

    const unreachable = new RuntimeEnvironmentRelayBridge({
      environmentId: 'env-unreachable-direct',
      deviceToken: harness.relayOffer.deviceToken,
      hostPublicKey: publicKeyFromBase64(harness.relayOffer.publicKeyB64),
      // Port 9 (discard) is closed on test hosts, so the direct attempt is refused quickly.
      directEndpoint: 'ws://127.0.0.1:9',
      readRoute: direct.readRoute,
      writeRoute: () => {},
      connectUrl: harness.connectUrl
    })
    cleanups.push(() => unreachable.dispose())
    await expect(
      statusThrough({
        ...direct.pairing,
        endpoint: unreachable.endpoint,
        publicKeyB64: unreachable.publicKeyB64
      })
    ).resolves.toMatchObject({ ok: true })
    expect(harness.state.clientConnections).toBe(relayDialsAfterPairing + 1)
  }, 20_000)

  it('refuses a link whose Relay host id does not belong to the pinned server key', async () => {
    const harness = await startHarness({ cellHonorsRevoke: true })
    const tampered = { ...harness.relayOffer.relay!, relayHostId: 'ZZZZZZZZZZZZZZZZ' }

    await expect(
      pairRuntimeEnvironmentThroughRelay(harness.relayOffer, tampered, {
        connectUrl: harness.connectUrl
      })
    ).resolves.toMatchObject({ ok: false, kind: 'access-link-invalid' })
    expect(harness.state.clientConnections).toBe(0)
  }, 20_000)
})
