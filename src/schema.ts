import { TSchema, Static } from '@sinclair/typebox';
import addFormats from 'ajv-formats';
import Ajv from 'ajv/dist/2019';

const ajv = addFormats(new Ajv({}), [
  'date-time',
  'time',
  'date',
  'email',
  'hostname',
  'ipv4',
  'ipv6',
  'uri',
  'uri-reference',
  'uuid',
  'uri-template',
  'json-pointer',
  'relative-json-pointer',
  'regex',
])
  .addKeyword('kind')
  .addKeyword('modifier');

export const createValidateFn = <T extends TSchema>(schema: T) => {
  const validate = ajv.compile<Static<T>>(schema);
  return validate;
};
