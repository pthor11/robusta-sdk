import { Consumer, Kafka } from "kafkajs";
import { loadSync } from "@grpc/proto-loader";
import { loadPackageDefinition, credentials } from "grpc";
import { join } from "path";
import { promisify } from "util";

type Currency = {
    type: 'btc' | 'bch' | 'ltc' | 'eth' | 'etc' | 'trx' | 'trc10' | 'trc20' | 'erc20',
    address: string | null
}

type Change = {
    address: string
    txid: string
    n: number
    value: string
    currency: Currency
    blockNumber: number
    timeStamp: number
}

class Robusta {
    private _apiKey: string
    private _provider: string
    private _grpc: any
    private _grpcCall: any
    private _kafka?: Kafka
    private _consumer?: Consumer
    private _callback?: (change: Change) => Promise<void>

    constructor(params: { apiKey: string, provider: string, brokers?: string[], callback?: (change: Change) => Promise<void> }) {
        if (!params.apiKey) throw new Error(`apiKey must be provided`)
        if (!params.provider) throw new Error(`provider must be provided`)

        this._apiKey = params.apiKey
        this._provider = params.provider
        this._callback = params.callback

        const packageObject = loadPackageDefinition(loadSync(join(__dirname, '../BrickService.proto'), {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        })) as any

        this._grpc = new packageObject['BrickService'](this._provider, credentials.createInsecure())

        this._grpcCall = promisify(this._grpc.call).bind(this._grpc)

        // this._kafka = new Kafka({
        //     clientId: this._apiKey,
        //     brokers: params.brokers,
        //     ssl: false,
        //     sasl: undefined
        // })

        // this._consumer = this._kafka.consumer({ groupId: `${this._apiKey}` })
    }

    // public async connect() {
    //     await this._consumer.connect()
    //     console.log(`consumer connected`)

    //     await this._consumer.subscribe({ topic: this._apiKey, fromBeginning: true })
    //     console.log(`topic subscribed`)

    //     await this._consumer.run({
    //         eachMessage: async payload => {
    //             const { value } = payload.message

    //             const data = JSON.parse(Buffer.from(value!).toString())

    //             this._callback(data)
    //         }
    //     })
    // }

    public async watch(params: { address: string, currency: Currency }): Promise<{ result: string, error: string }> {
        try {
            const { result, error } = await this._grpcCall({
                method: 'user_account_watch',
                params: JSON.stringify({ apiKey: this._apiKey, ...params })
            })

            // console.log({ result, error });

            return { result, error }
        } catch (e) {
            throw e
        }
    }

    public async unwatch(params: { address: string, currency: Currency }): Promise<{ result: string, error: string }> {
        try {
            const { result, error } = await this._grpcCall({
                method: 'user_account_unwatch',
                params: JSON.stringify({ apiKey: this._apiKey, ...params })
            })

            // console.log({ result, error });

            return { result, error }
        } catch (e) {
            throw e
        }
    }

}

export { Robusta }