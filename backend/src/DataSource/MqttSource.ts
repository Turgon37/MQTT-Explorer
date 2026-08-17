import { URL } from 'url'
import { readFileSync } from 'fs'

import { type MqttClient, connect as mqttConnect } from 'mqtt'
import { DataSource, DataSourceStateMachine } from './'
import { MqttMessage } from '../../../events'
import { Base64Message } from '../Model/Base64Message'

export interface MqttOptions {
  url: string
  username?: string
  password?: string
  tls: boolean
  certValidation: boolean
  clientId?: string
  subscriptions: Array<Subscription>
  certificateAuthority?: string
  certificateAuthorityPath?: string
  clientCertificate?: string
  clientCertificatePath?: string
  clientKey?: string
  clientKeyPath?: string
}

export interface Subscription {
  topic: string
  qos: QoS
}

export type QoS = 0 | 1 | 2

function resolveCertificateValue(encodedData?: string, filePath?: string): Buffer | undefined {
  if (encodedData) {
    return Buffer.from(encodedData, 'base64')
  }

  if (filePath) {
    return readFileSync(filePath)
  }

  return undefined
}

export class MqttSource implements DataSource<MqttOptions> {
  public stateMachine: DataSourceStateMachine = new DataSourceStateMachine()
  private client: MqttClient | undefined
  private messageCallback?: (topic: string, message: Buffer, packet: any) => void
  public topicSeparator = '/'

  public onMessage(messageCallback: (topic: string, message: Buffer, packet: any) => void) {
    this.messageCallback = messageCallback
  }

  public connect(options: MqttOptions): DataSourceStateMachine {
    this.stateMachine.setConnecting()

    const urlStr = options.tls ? options.url.replace(/^(mqtt|ws):/, '$1s:') : options.url
    let url
    try {
      url = new URL(urlStr)
    } catch (error) {
      this.stateMachine.setError(error as Error)
      throw error
    }

    let certificateAuthority
    let clientCertificate
    let clientKey

    try {
      certificateAuthority = resolveCertificateValue(options.certificateAuthority, options.certificateAuthorityPath)
      clientCertificate = resolveCertificateValue(options.clientCertificate, options.clientCertificatePath)
      clientKey = resolveCertificateValue(options.clientKey, options.clientKeyPath)
    } catch (error) {
      this.stateMachine.setError(error as Error)
      throw error
    }

    const client = mqttConnect(url.toString(), {
      resubscribe: false,
      rejectUnauthorized: options.certValidation,
      username: options.username,
      password: options.password,
      clientId: options.clientId,
      servername: options.tls ? url.hostname : undefined,
      ca: certificateAuthority,
      cert: clientCertificate,
      key: clientKey,
    } as any)

    this.client = client

    client.on('error', (error: Error) => {
      console.log(error)
      this.stateMachine.setError(error)
    })

    client.on('close', () => {
      this.stateMachine.setConnected(false)
    })

    client.on('end', () => {
      this.stateMachine.setConnected(false)
    })

    client.on('reconnect', () => {
      this.stateMachine.setConnecting()
    })

    client.on('connect', () => {
      this.stateMachine.setConnected(true)
      options.subscriptions.forEach(subscription => {
        client.subscribe(subscription.topic, { qos: subscription.qos }, (err: Error | null) => {
          if (err) {
            this.stateMachine.setError(err)
          }
        })
      })
    })

    client.on('message', (topic, message, packet) => {
      this.messageCallback && this.messageCallback(topic, message, packet)
    })

    return this.stateMachine
  }

  public publish(msg: MqttMessage) {
    if (this.client) {
      this.client.publish(msg.topic, (msg.payload && new Base64Message(msg.payload))?.toBuffer() ?? '', {
        qos: msg.qos,
        retain: msg.retain,
      })
    }
  }

  public disconnect() {
    this.client && this.client.end()
  }
}
