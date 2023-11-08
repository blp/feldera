import { AttachedConnector, ConnectorDescr, ProgramDescr } from '$lib/services/manager'

export enum ConnectorType {
  KAFKA_IN = 'KafkaIn',
  KAFKA_OUT = 'KafkaOut',
  DEBEZIUM_IN = 'DebeziumIn',
  SNOWFLAKE_OUT = 'SnowflakeOut',
  URL = 'HTTP_GET',
  UNKNOWN = 'Unknown'
}

export enum Direction {
  INPUT = 'input',
  OUTPUT = 'output',
  INPUT_OUTPUT = 'input_output'
}

export type IONodeData = {
  connector: ConnectorDescr
  ac: AttachedConnector
}

export type ProgramNodeData = {
  label: string
  program: ProgramDescr
}
