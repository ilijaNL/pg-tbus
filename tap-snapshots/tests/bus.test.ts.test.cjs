/* IMPORTANT
 * This snapshot file is auto-generated, but designed for humans.
 * It should be checked into source control and tracked carefully.
 * Re-generate by setting TAP_SNAPSHOT=1 and running tests.
 * Make sure to inspect the output below.  Do not ignore changes!
 */
'use strict'
exports[`tests/bus.test.ts TAP getState > must match snapshot 1`] = `
{
  "events": [
    {
      "event_name": "eventA",
      "schema": {
        "properties": {
          "c": {
            "type": "number"
          }
        },
        "required": [
          "c"
        ],
        "type": "object"
      }
    },
    {
      "event_name": "eventB",
      "schema": {
        "properties": {
          "d": {
            "type": "number"
          }
        },
        "required": [
          "d"
        ],
        "type": "object"
      }
    }
  ],
  "queue": "doesnot-matter",
  "tasks": [
    {
      "config": {},
      "schema": {
        "properties": {
          "works": {
            "type": "string"
          }
        },
        "required": [
          "works"
        ],
        "type": "object"
      },
      "task_name": "task_a"
    },
    {
      "config": {},
      "schema": {
        "properties": {
          "works": {
            "type": "string"
          }
        },
        "required": [
          "works"
        ],
        "type": "object"
      },
      "task_name": "task_b"
    },
    {
      "config": {},
      "on_event": "eventA",
      "schema": {
        "properties": {
          "c": {
            "type": "number"
          }
        },
        "required": [
          "c"
        ],
        "type": "object"
      },
      "task_name": "task_event_a"
    },
    {
      "config": {},
      "on_event": "eventB",
      "schema": {
        "properties": {
          "d": {
            "type": "number"
          }
        },
        "required": [
          "d"
        ],
        "type": "object"
      },
      "task_name": "task_event_b"
    }
  ]
}
`

exports[`tests/bus.test.ts TAP taskclient > must match snapshot 1`] = `
{
  "events": [],
  "queue": "queueA",
  "tasks": [
    {
      "config": {
        "retryDelay": 20
      },
      "schema": {
        "properties": {
          "n": {
            "minimum": 2,
            "type": "number"
          }
        },
        "required": [
          "n"
        ],
        "type": "object"
      },
      "task_name": "test"
    },
    {
      "config": {
        "keepInSeconds": 8,
        "retryDelay": 10
      },
      "schema": {
        "properties": {
          "n": {
            "type": "string"
          }
        },
        "required": [
          "n"
        ],
        "type": "object"
      },
      "task_name": "abc"
    }
  ]
}
`
