const Promise = require('bluebird')
const csvParser = require('csv-parser')
const _ = require('lodash')
const fs = require('fs')
const path = require('path')
const { Parser } = require('@json2csv/plainjs')
const json2csv = new Parser()
const moment = require('moment')

async function readCsv (csvFile) {
  return await new Promise(function (resolve, reject) {
    const results = []
    fs.createReadStream(csvFile)
      .pipe(csvParser())
      .on('data', (data) => results.push(data))
      .on('end', function () {
        resolve(results)
      })
  })
}

async function main () {
  let unitData = await readCsv(process.argv[2])

  unitData = _.chain(unitData)
    .filter('unit')
    .map(function (row) {
      row.apt = `${row.floor}${row.unit}`
      row.sold = 'FALSE'

      return _.mapValues(row, function (val) {
        return val ? val.trim().replace('$', '').replace(/,/g, '').replace(/\*/g, '') : val
      })
    })
    .keyBy('apt')
    .value()

  let acrisData = await readCsv(path.join(__dirname, 'acris.csv'))

  acrisData = _.chain(acrisData)
    .filter('unit')
    .filter((row) => {
      return _.includes(['DEED', 'MTGE'], row.doc_type)
    })
    .groupBy('unit')
    .mapValues((docs) => {
      return _.keyBy(docs, 'doc_type')
    })
    .value()

  _.each(acrisData, (data, unit) => {
    _.assign(unitData[unit], {
      sold: !!data.DEED?.unit,
      sold_on: moment(data.DEED?.document_date).format('MM/DD/YYYY'),
      sold_for: data.DEED?.document_amt,
      mortgage: !!data.MTGE?.unit,
      mortgage_amount: data.MTGE?.document_amt
    })
  })

  const csvData = await json2csv.parse(_.values(unitData))
  await fs.promises.writeFile('report.csv', csvData)
}

main()
