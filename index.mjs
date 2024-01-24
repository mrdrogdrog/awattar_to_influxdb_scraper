import {InfluxDB, Point} from "@influxdata/influxdb-client";
import {config} from "dotenv";
import {DateTime} from "luxon";

config()

const token = process.env.INFLUXDB_TOKEN
const url = process.env.INFLUXDB_URL
const org = process.env.INFLUXDB_ORG;
const bucket = process.env.INFLUXDB_BUCKET;
const client = new InfluxDB({url, token})

async function findLastDate(client, fieldName) {
    const res = await client.getQueryApi(org).collectRows(`from(bucket: "awattar")
  |> range(start: -100000h, stop: 100000h)
  |> filter(fn: (r) => r["_measurement"] == "${fieldName}")
  |> last()
  |> sort(columns: ["_time"])`)

    if (res.length > 0) {
        return DateTime.fromISO(res[0]._time).plus(1)
    }
    return DateTime.now().minus({day: 1})
}

async function doAwattarApiRequest(endpoint, start, end) {
    console.log("Fetching data from", start.toString(), "to", end.toString())
    const url = new URL(`https://api.awattar.de/v1/${endpoint}`)
    url.searchParams.set("start", start.toMillis())
    url.searchParams.set("end", end.toMillis())
    const response = await fetch(url)
    return await response.json()
}

function convertMarketPriceToPoint(datapoint) {
    if (datapoint.marketprice === null) {
        return
    }
    return new Point('marketprice')
        .timestamp(new Date(datapoint.start_timestamp))
        .stringField("unit", datapoint.unit)
        .floatField('marketprice', datapoint.marketprice)
}

function convertProductionToPoint(datapoint) {
    if (datapoint.solar === null || datapoint.wind === null) {
        return
    }
    debugger
    return new Point('production')
        .timestamp(new Date(datapoint.start_timestamp))
        .floatField('solar', datapoint.solar)
        .floatField('wind', datapoint.wind)
}

function processDataPoints(datapoints, writeClient, pointFactory) {
    if (datapoints.length === 0) {
        return
    }
    for (const datapoint of datapoints) {
        const point = pointFactory.call(this, datapoint)
        if (point === undefined) {
            continue
        }
        writeClient.writePoint(point)
    }
}


const writeClient = client.getWriteApi(org, bucket, 'ns');
const endOfToday = DateTime.now().endOf("day");
const endOfTomorrow =  DateTime.now().plus({day: 1}).endOf("day");

const lastMarketPriceDate = await findLastDate(client, 'marketprice');
const marketdata = await doAwattarApiRequest('marketdata', lastMarketPriceDate, endOfTomorrow)
console.log("found", marketdata.data.length, "new marketdata entries")
processDataPoints(marketdata.data, writeClient, convertMarketPriceToPoint)


const lastProductionDate = await findLastDate(client, 'production');
const production = await doAwattarApiRequest('power/productions', lastProductionDate, endOfToday)
console.log("found", production.data.length, "new production entries")
processDataPoints(production.data, writeClient, convertProductionToPoint)

await writeClient.flush()
