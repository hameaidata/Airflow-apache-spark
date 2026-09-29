CREATE TABLE BDS_SALDO_OPERATIVO (
  ID_OPERACION_CIERRE longtext NOT NULL 
    COMMENT 'Identificador único de la operación al cierre del periodo procesado. Permite identificar de forma unívoca el saldo operativo registrado.',
  FECHA_PROCESO date NOT NULL 
    COMMENT 'Fecha de proceso o fecha de corte sobre la cual se calculan y registran los saldos e indicadores de la operación.',
  COD_EMPRESA int(11) DEFAULT NULL 
    COMMENT 'Código de la empresa o entidad financiera propietaria de la operación.',
  COD_SUCURSAL int(11) DEFAULT NULL 
    COMMENT 'Código de la sucursal, oficina o agencia donde se originó o administra la operación.',
  COD_RUBRO varchar(20) DEFAULT NULL 
    COMMENT 'Código del rubro contable o segmento financiero utilizado para clasificar la operación.',
  COD_MONEDA int(11) DEFAULT NULL 
    COMMENT 'Código de la moneda de origen de la operación según catálogo corporativo.',
  COD_PAPEL int(11) DEFAULT NULL 
    COMMENT 'Código del tipo de papel financiero, instrumento o clasificación interna asociada a la operación.',
  NUM_CUENTA_BT int(11) DEFAULT NULL 
    COMMENT 'Número de cuenta contable o cuenta bancaria interna asociada al registro.',
  COD_OPERACION int(11) DEFAULT NULL 
    COMMENT 'Código principal de la operación financiera registrado en los sistemas transaccionales.',
  COD_SUB_OPERACION int(11) DEFAULT NULL 
    COMMENT 'Código de suboperación que permite una clasificación más específica dentro de la operación principal.',
  COD_TIPO_OPERACION int(11) DEFAULT NULL 
    COMMENT 'Código que identifica la naturaleza o tipo de operación financiera.',
  COD_MODULO int(11) DEFAULT NULL 
    COMMENT 'Código del módulo aplicativo o sistema origen de donde proviene la operación.',
  FEC_VENCIMIENTO date DEFAULT NULL 
    COMMENT 'Fecha de vencimiento contractual de la operación o producto financiero.',
  FEC_VALOR date DEFAULT NULL 
    COMMENT 'Fecha valor utilizada para efectos financieros y cálculo de intereses.',
  IND_CATEGORIA_RIESGO varchar(15) DEFAULT NULL 
    COMMENT 'Indicador o clasificación de riesgo asignado a la operación según políticas crediticias.',
  COD_ACTI_BCO_CENTRAL bigint(20) DEFAULT NULL 
    COMMENT 'Código de actividad económica reportada al Banco Central o entidad reguladora.',
  COD_PRODUCTO int(11) DEFAULT NULL 
    COMMENT 'Código del producto financiero asociado a la operación.',
  MTO_SALDO_ORIGEN decimal(17,2) DEFAULT NULL 
    COMMENT 'Saldo de la operación expresado en la moneda de origen.',
  MTO_SALDO_MN decimal(17,2) DEFAULT NULL 
    COMMENT 'Saldo de la operación convertido a moneda nacional.',
  MTO_SALDO_ME decimal(17,2) DEFAULT NULL 
    COMMENT 'Saldo de la operación convertido a moneda extranjera.',
  MTO_SALDO_MO decimal(17,2) DEFAULT NULL 
    COMMENT 'Saldo de la operación expresado en moneda operativa o moneda de reporte.',
  MTO_INTERES decimal(17,2) DEFAULT NULL 
    COMMENT 'Monto acumulado de intereses asociados a la operación al cierre del periodo.',
  MTO_PREVISIONES decimal(17,2) DEFAULT NULL 
    COMMENT 'Monto de provisiones o previsiones constituidas para cubrir riesgos de la operación.',
  AVG_SALDO_ORIGEN decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio del periodo calculado en moneda de origen.',
  AVG_SALDO_MN decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio del periodo expresado en moneda nacional.',
  AVG_SALDO_ME decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio del periodo expresado en moneda extranjera.',
  AVG_SALDO_MO decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio del periodo expresado en moneda operativa o de reporte.',
  AVG_SALDO_ORIGEN_LABORABLES decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio calculado únicamente sobre días laborables en moneda de origen.',
  AVG_SALDO_MN_LABORABLES decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio calculado únicamente sobre días laborables en moneda nacional.',
  AVG_SALDO_ME_LABORABLES decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio calculado únicamente sobre días laborables en moneda extranjera.',
  AVG_SALDO_MO_LABORABLES decimal(17,4) DEFAULT NULL 
    COMMENT 'Saldo promedio calculado únicamente sobre días laborables en moneda operativa.',
  MTO_SALDO_VIGENTE_MO decimal(21,2) DEFAULT NULL 
    COMMENT 'Monto del saldo vigente de la operación en moneda operativa.',
  MTO_SALDO_RESTRUCTURADO_MO decimal(21,2) DEFAULT NULL 
    COMMENT 'Monto del saldo correspondiente a operaciones reestructuradas en moneda operativa.',
  MTO_SALDO_REFINANCIADO_MO decimal(21,2) DEFAULT NULL 
    COMMENT 'Monto del saldo correspondiente a operaciones refinanciadas en moneda operativa.',
  MTO_SALDO_VENCIDO_MO decimal(21,2) DEFAULT NULL 
    COMMENT 'Monto del saldo vencido de la operación en moneda operativa.',
  MTO_SALDO_JUDIAL_MO decimal(21,2) DEFAULT NULL 
    COMMENT 'Monto del saldo transferido a cobranza judicial o proceso legal en moneda operativa.',
  FUENTE varchar(250) DEFAULT NULL 
    COMMENT 'Nombre del sistema, archivo o proceso origen desde donde se obtuvo la información.',
  FECHA_CARGA datetime DEFAULT NULL 
    COMMENT 'Fecha y hora en que el registro fue cargado al repositorio de datos.',
  BATCH_ID varchar(25) DEFAULT NULL 
    COMMENT 'Identificador del lote de carga o proceso ETL responsable de insertar el registro.',
  SHARD KEY (ID_OPERACION, FECHA_PROCESO),
  CONSTRAINT UK_BDS_SALDO_OPERATIVO 
    UNIQUE(ID_OPERACION_CIERRE, FECHA_PROCESO)
)
COMMENT='Tabla de saldos operativos diarios de productos y operaciones financieras. Contiene saldos, intereses, provisiones, métricas promedio e indicadores de riesgo para fines regulatorios, financieros y analíticos.';