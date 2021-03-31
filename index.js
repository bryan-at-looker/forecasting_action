const util = require('util');
const moment = require('moment')
const { LookerNodeSDK } = require('@looker/sdk-node')
const sdk = LookerNodeSDK.init40()

const url = "https://a4607af6fbe3.ngrok.io";
var num_regexp = /^-?\d+\.?\d*$/


exports.helloWorld = async (req, res) => {
  if (process.env.LOOKER_WEBHOOK_TOKEN !== req.headers['x-looker-webhook-token']) {
    return res.status('401').send('Unauthorized')
  }
  const path = req.path.split('/')
  switch (path[1]) {
    case 'data_action':
      if (path[path.length-1] === 'form') {
        return forecastForm(req,res);
      } else {
        return dataAction(req, res, path);
      }
    default:
      return res.status('401').send('Unauthorized');
  }
};

async function forecastForm (req, res) {
  
  const fields = [
    {
      name: "start_date",
      label: "Start Date",
      type: 'text',
      required: true,
      description: "Enter date in the format of 2021-11-25",
      default: moment().add(1, 'days').format('YYYY-MM-DD')
    },
    {
      name: "end_date",
      label: "End Date",
      type: 'text',
      required: true,
      description: "Enter date in the format of 2021-11-25",
      default: moment().add(1, 'years').format('YYYY-MM-DD')
    },
    {
      name: "amount",
      label: "Forecast Amount",
      type: 'text',
      required: true,
      description: "What amount do you want to increase by",
      default: '31'
    },
    {
      name: "spread_type",
      label: "Spread Type",
      type: 'select',
      required: true,
      description: "What type of function do you want to apply to your time range",
      default: 'even',
      options: [
        {name: 'even', label: 'Even'},
        {name: 'linear', label: 'Linear'},
        {name: 'one time', label: 'One Time'},
        {name: 'sigmoid', label: 'Sigmoid'}
      ]
    },
    {
      name: "continue",
      label: "Continue after end?",
      type: 'select',
      required: true,
      description: "After the end date, do you want to continue the forecast at that amount",
      default: 'true',
      options: [
        {name: 'true', label: 'Yes'},
        {name: 'false', label: 'No'}
      ]
    }
  ]
  res.send({
    fields
  })
}

async function dataAction (req, res, path) {
  const { body } = req;
  const { data, form_params } = body;
  if ( ! (form_params.amount && num_regexp.test(form_params.amount) ) ) {
    console.log({amount: form_params.amount, test: num_regexp.test(form_params.amount)});
    return res.status('402').send(validationError('amount','Amount field must be a number'))
  }

  const field_list = [
    {
      name: 'looker_user_id',
      sql: 'CAST( %s as INT64)',
      value: data.looker_user_id
    },
    {
      name: 'category',
      value: data.category,
      sql: "'%s'",
    },
    {
      name: 'continue',
      value: form_params.continue,
      sql: "%s",
    },
    {
      name: 'spread_type',
      value: form_params.spread_type,
      sql: "'%s'",
    },
    {
      name: 'amount',
      value: form_params.amount,
      sql: "%s",
    },
    {
      name: 'start_date',
      value: form_params.start_date,
      sql: "CAST('%s' as TIMESTAMP)",
    },
    {
      name: 'end_date',
      value: form_params.end_date,
      sql: "CAST('%s' as TIMESTAMP)",
    }
  ]
  await insertRow(field_list, data.view, data.model)
  res.send({
    "looker": {
      "success": true,
      "refresh_query": true
    }
  })
}

async function insertRow (fields, view, model) {
  const SQL = `
INSERT INTO $\{${view}.SQL_TABLE_NAME}
( 
  updated_at,
  ${fields.map(f=>f.name).join(', ')} 
)
VALUES ( 
  CURRENT_TIMESTAMP(),
  ${fields.map(f=>{ return util.format(f.sql, f.value) }).join(',\n')} 
) 
  `
  try {
    const create = await sdk.ok(sdk.create_sql_query({
      model_name: model,
      sql: SQL
    }))
    
    return sdk.ok(sdk.run_sql_query(create.slug, 'json'));
  } catch (e) {
    console.error(e);
    return e.message
  }
}

function validationError(field, reason) {
  return {
    looker: {
      success: false,
      validation_errors: {
        [field]: reason
      }
    }
  }
}