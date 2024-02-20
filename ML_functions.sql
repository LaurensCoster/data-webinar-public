## GEN AI
CREATE OR REPLACE MODEL
`data-webinar.ga4_webinar.llm`
REMOTE WITH CONNECTION `data-webinar.eu.bq-vertexai-connection`
OPTIONS(ENDPOINT = "text-bison");

SELECT *
FROM
  ML.GENERATE_TEXT(
    MODEL `data-webinar.ga4_webinar.llm`,
    (SELECT 'Dlaczego warto przerzucić dane z GA4 do BigQuery?' AS prompt),
    STRUCT(
      0.8 AS temperature,
      1024 AS max_output_tokens,
      0.95 AS top_p,
      40 AS top_k));


###NLP 
CREATE OR REPLACE MODEL
`data-webinar.ga4_webinar.nlp`
REMOTE WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS(REMOTE_SERVICE_TYPE = "CLOUD_AI_NATURAL_LANGUAGE_V1");

SELECT 
  *
FROM ML.UNDERSTAND_TEXT(
  MODEL `data-webinar.ga4_webinar.nlp`,
  (SELECT 'Niska cena, niskie oczekiwania. Byłem 3 razy. Jeśli nie będę miał innej możliwości, będę czwarty raz.' as text_content),
  STRUCT('ANALYZE_SENTIMENT' AS nlu_option)
); #4/5 na Google


###TRANSLATE 
CREATE OR REPLACE MODEL `data-webinar.ga4_webinar.translate`
REMOTE WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS(REMOTE_SERVICE_TYPE = "CLOUD_AI_TRANSLATE_V3");

SELECT * from ML.TRANSLATE(
  MODEL `data-webinar.ga4_webinar.translate`,
  (SELECT 'Niska cena, niskie oczekiwania. Byłem 3 razy. Jeśli nie będę miał innej możliwości, będę czwarty raz.' AS text_content),
  STRUCT('translate_text' AS translate_mode, 'en' AS target_language_code)
);

#EMBEDDINGS 
CREATE MODEL `data-webinar.ga4_webinar.embeddings`
 REMOTE WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
 OPTIONS(ENDPOINT = 'textembedding-gecko@latest');

SELECT * FROM
  ML.GENERATE_TEXT_EMBEDDING(
    MODEL `data-webinar.ga4_webinar.embeddings`,
    (SELECT 'Niska cena, niskie oczekiwania. Byłem 3 razy. Jeśli nie będę miał innej możliwości, będę czwarty raz.' AS content),
    STRUCT(TRUE AS flatten_json_output)
);

#TRANSCRIPTIONS 
CREATE or replace MODEL `data-webinar.ga4_webinar.transcription`
 REMOTE WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
 OPTIONS(REMOTE_SERVICE_TYPE = 'CLOUD_AI_SPEECH_TO_TEXT_V2');

CREATE EXTERNAL TABLE `data-webinar.ga4_webinar.transcription_objects`
 WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS(
  object_metadata = 'SIMPLE',
  uris = ['gs://bq-webinar-transcription/*'],
  max_staleness = INTERVAL 1 DAY,
  metadata_cache_mode = 'AUTOMATIC'
);

select * from `data-webinar.ga4_webinar.transcription_objects`;

SELECT *
FROM ML.TRANSCRIBE(
  MODEL `data-webinar.ga4_webinar.transcription`,
  TABLE `data-webinar.ga4_webinar.transcription_objects`,
  recognition_config => ( JSON '{"language_codes": ["pl-PL" ],"model": "long","auto_decoding_config": {}}')
);

#DOCUMENT PROCESSING
CREATE OR REPLACE MODEL `data-webinar.ga4_webinar.documents`
REMOTE WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS (remote_service_type = 'cloud_ai_document_v1',
document_processor='projects/181671615042/locations/eu/processors/fe73e92255c89f89/processorVersions/pretrained-invoice-v2.0-2023-12-06');

CREATE EXTERNAL TABLE `data-webinar.ga4_webinar.invoices`
 WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS(
  object_metadata = 'SIMPLE',
  uris = ['gs://bq-webinar-invoices/*'],
  max_staleness = INTERVAL 1 DAY,
  metadata_cache_mode = 'AUTOMATIC'
);

select * from `data-webinar.ga4_webinar.invoices`;

SELECT *
FROM ML.PROCESS_DOCUMENT(
  MODEL `data-webinar.ga4_webinar.documents`,
  TABLE `data-webinar.ga4_webinar.invoices`
);

#IMAGE
CREATE OR REPLACE MODEL `data-webinar.ga4_webinar.images_annotation`
REMOTE WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS (remote_service_type = 'CLOUD_AI_VISION_V1');

CREATE EXTERNAL TABLE `data-webinar.ga4_webinar.images`
 WITH CONNECTION `projects/data-webinar/locations/eu/connections/bq-vertexai-connection`
OPTIONS(
  object_metadata = 'SIMPLE',
  uris = ['gs://bq-webinar-images/*'],
  max_staleness = INTERVAL 1 DAY,
  metadata_cache_mode = 'AUTOMATIC'
);

select * from `data-webinar.ga4_webinar.images`;

SELECT *
FROM ML.ANNOTATE_IMAGE(
  MODEL `data-webinar.ga4_webinar.images_annotation`,
  TABLE `data-webinar.ga4_webinar.images`,
  STRUCT(['label_detection'] AS vision_features)
);
