const Promise = require('bluebird')
const request = Promise.promisify(require('request'))
const _ = require('lodash')
const { Parser } = require('@json2csv/plainjs')
const json2csv = new Parser()
const fs = require('fs')
require('dotenv').config()

async function apiRequest (opts) {
  const resp = await request(_.defaultsDeep(opts, {
    method: 'GET',
    baseUrl: 'https://data.cityofnewyork.us/resource',
    headers: {
      'X-App-Token': process.env.NYC_OPENDATA_APP_TOKEN
    },
    qs: {
      $limit: 50000
    },
    json: true
  }))

  return resp.body
}

async function getLegalsByAddress (streetNumber, streetName) {
  const data = await apiRequest({
    url: '/8h5j-fqxa.json',
    qs: {
      street_number: streetNumber,
      street_name: streetName
    }
  })

  return data
}

async function getPropertyMasterByDocumentId (documentId) {
  const data = await apiRequest({
    url: '/bnx9-e6tj.json',
    qs: {
      document_id: documentId
    }
  })

  return _.first(data)
}

async function main () {
  const legals = await getLegalsByAddress('11', 'HOYT STREET')

  await Promise
    .resolve(legals)
    .map(async function (legal) {
      console.log(legal.unit, legal.document_id)

      const master = await getPropertyMasterByDocumentId(legal.document_id)
      _.assign(legal, _.pick(master, [
        'doc_type',
        'document_amt',
        'document_date'
      ]))

      return legal
    }, {
      concurrency: 4
    })

  const csvData = await json2csv.parse(legals)
  await fs.promises.writeFile('acris.csv', csvData)
}

main()
