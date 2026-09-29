CREATE TABLE  BDS_SEGMENTOS  (
   FECHA_PROCESO    date         NOT NULL COMMENT '',
   COD_EMPRESA      decimal(3,0) NOT NULL COMMENT 'Codigo de empresa',
   NUM_CODIGO       decimal(5,0) NOT NULL COMMENT 'Numero de codigo de reporte',
   COD_TIPO         varchar(16)  NOT NULL COMMENT 'Codigo tipo',
   COD_SEGMENTO     varchar(16)  NOT NULL COMMENT 'Codigo de Segemento',
   BC206ID3         varchar(16)  NOT NULL COMMENT '',
   BC206ID4         varchar(16)  NOT NULL COMMENT '',
   NOM_ABREVIADO    varchar(40)  DEFAULT NULL COMMENT 'Nombre abreviado',
   DES_NOMBRE       varchar(40)  DEFAULT NULL COMMENT 'Descripcion de nombre',
   FUENTE           varchar(250) DEFAULT 'BANTOTAL' COMMENT 'Fuente de informacion' ,
   FECHA_CARGA      datetime     DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha de carga',
   BATCH_ID         varchar(25)  DEFAULT NULL COMMENT '',
  SHARD KEY  ( COD_EMPRESA , NUM_CODIGO , COD_TIPO , COD_SEGMENTO , BC206ID3 , BC206ID4 ),
  CONSTRAINT  UK_ODS_SEGMENTOS_01  UNIQUE ( COD_EMPRESA , NUM_CODIGO , COD_TIPO , COD_SEGMENTO , BC206ID3 , BC206ID4 )
)