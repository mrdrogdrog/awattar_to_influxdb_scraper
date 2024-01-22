import {InfluxDB, Point} from "@influxdata/influxdb-client";

const token = process.env.INFLUXDB_TOKEN
const url = process.env.INFLUXDB_URL
const org = process.env.INFLUXDB_ORG;
const bucket = process.env.INFLUXDB_BUCKET;
const client = new InfluxDB({url, token})

async function findLastDate(client, fieldName) {
    const res = await client.getQueryApi(org).collectRows(`from(bucket: "awattar")
  |> range(start: 0h, stop: 100000h)
  |> filter(fn: (r) => r["_measurement"] == "${fieldName}")
  |> last()`)

    if (res.length > 0) {
        return new Date(new Date(res[0]._time).getTime() + 1)
    }
    return new Date(new Date().getTime() - 24*60*60*1000)
}

async function doAwattarApiRequest(endpoint, lastDate) {
    console.log("Fetching data after", lastDate.toISOString())
    const url = new URL(`https://api.awattar.de/v1/${endpoint}`)
    url.searchParams.set("start", lastDate.getTime())
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

const lastMarketPriceDate = await findLastDate(client, 'marketprice');
const marketdata = await doAwattarApiRequest('marketdata', lastMarketPriceDate)
console.log("found", marketdata.data.length, "new marketdata entries")
processDataPoints(marketdata.data, writeClient, convertMarketPriceToPoint)


const lastProductionDate = await findLastDate(client, 'production');
const production = await doAwattarApiRequest('power/productions', lastProductionDate)
console.log("found", production.data.length, "new production entries")
processDataPoints(production.data, writeClient, convertProductionToPoint)

await writeClient.flush()
