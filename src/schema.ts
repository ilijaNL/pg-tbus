import { TSchema, Type, Static } from '@sinclair/typebox';
import addFormats from 'ajv-formats';
import Ajv from 'ajv/dist/2019';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);
export const Uuid = Type.String({ format: 'uuid' });

// no validation
export function createData<T extends TSchema>(_: T, data: Static<T>) {
  return data;
}

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
