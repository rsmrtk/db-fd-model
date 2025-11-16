CREATE TABLE airlines (
  airline_id STRING(36) NOT NULL,
  name STRING(256) NOT NULL,
  name_key STRING(256) NOT NULL,
  iata STRING(2),
  icao STRING(3),
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(airline_id);

CREATE TABLE airports (
  airport_id STRING(36) NOT NULL,
  name STRING(256) NOT NULL,
  name_key STRING(256) NOT NULL,
  iata STRING(3) NOT NULL,
  icao STRING(4) NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  polygon JSON NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(airport_id);

CREATE TABLE airport_airlines (
  airport_id STRING(36) NOT NULL,
  airline_id STRING(36) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(airport_id, airline_id),
  INTERLEAVE IN PARENT airports ON DELETE CASCADE;

CREATE TABLE airport_airline_terminals (
  airport_id STRING(36) NOT NULL,
  airline_id STRING(36) NOT NULL,
  terminal_id STRING(36) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(airport_id, airline_id, terminal_id),
  INTERLEAVE IN PARENT airport_airlines ON DELETE CASCADE;

CREATE TABLE airport_airline_terminal_zones (
  airport_id STRING(36) NOT NULL,
  airline_id STRING(36) NOT NULL,
  terminal_id STRING(36) NOT NULL,
  zone_id STRING(36) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(airport_id, airline_id, terminal_id, zone_id),
  INTERLEAVE IN PARENT airport_airline_terminals ON DELETE CASCADE;

CREATE TABLE airport_terminals (
  airport_id STRING(36) NOT NULL,
  terminal_id STRING(36) NOT NULL,
  name STRING(256) NOT NULL,
  name_key STRING(256) NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(airport_id, terminal_id),
  INTERLEAVE IN PARENT airports ON DELETE CASCADE;

ALTER TABLE airport_airline_terminal_zones ADD FOREIGN KEY(terminal_id) REFERENCES airport_terminals(terminal_id);

ALTER TABLE airport_airline_terminals ADD FOREIGN KEY(terminal_id) REFERENCES airport_terminals(terminal_id);

CREATE TABLE airport_terminal_zones (
  airport_id STRING(36) NOT NULL,
  terminal_id STRING(36) NOT NULL,
  zone_id STRING(36) NOT NULL,
  name STRING(256) NOT NULL,
  name_key STRING(256) NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  arrival BOOL,
  departure BOOL,
  --ENUM(active, inactive, deleted) COLUMN status
  status STRING(32) NOT NULL DEFAULT ('active'),
) PRIMARY KEY(airport_id, terminal_id, zone_id),
  INTERLEAVE IN PARENT airport_terminals ON DELETE CASCADE;

ALTER TABLE airport_airline_terminal_zones ADD FOREIGN KEY(zone_id) REFERENCES airport_terminal_zones(zone_id);

ALTER TABLE airport_airline_terminal_zones ADD FOREIGN KEY(zone_id) REFERENCES airport_terminal_zones(zone_id);

CREATE TABLE customer_welcome_emails (
  welcome_email_id STRING(36) NOT NULL,
  to_ STRING(255) NOT NULL,
  subject STRING(255) NOT NULL,
  body STRING(MAX) NOT NULL,
  sent_at INT64 NOT NULL,
  sent_successful BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(welcome_email_id);

CREATE TABLE customers (
  customer_id STRING(36) NOT NULL,
  first_name STRING(256) NOT NULL,
  last_name STRING(256) NOT NULL,
  email STRING(64) NOT NULL,
  phone STRING(64) NOT NULL,
  selfie_url STRING(1024),
  selfie_thumb_url STRING(1024),
  stripe_id STRING(36),
  --ENUM(active, inactive, deleted) COLUMN status
  status STRING(32) NOT NULL,
  jurny_client_id INT64,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  developer BOOL DEFAULT (FALSE),
  vip BOOL DEFAULT (FALSE),
) PRIMARY KEY(customer_id);

CREATE UNIQUE INDEX idx_customers_phone ON customers(phone) STORING (customer_id, status);

CREATE TABLE customer_accounts (
  customer_id STRING(36) NOT NULL,
  account_id STRING(36) NOT NULL,
  jurny_account_id STRING(36) NOT NULL,
  name STRING(128) NOT NULL,
  cc_required BOOL NOT NULL,
  --ENUM(personal, business) COLUMN type
  type STRING(32) NOT NULL,
  active BOOL NOT NULL,
  jurny_client_id INT64,
  jurny_company_id INT64,
  default_ BOOL,
  deleted BOOL NOT NULL,
  default_payment_method_id STRING(36),
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  total_weekly_spend NUMERIC,
  limit_enable BOOL,
) PRIMARY KEY(customer_id, account_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

CREATE TABLE customer_ride_guests (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  guest_id STRING(36) NOT NULL,
  token STRING(32) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, ride_id, guest_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_balance (
 customer_id      STRING(36)   NOT NULL,
 current_balance  NUMERIC      NOT NULL,
 created_at       TIMESTAMP    NOT NULL,
 updated_at       TIMESTAMP
) PRIMARY KEY(customer_id), 
INTERLEAVE IN PARENT customers;

CREATE TABLE customer_balance_transactions (
    customer_id       STRING(36)   NOT NULL,
    transaction_id    STRING(36)   NOT NULL,
    amount            NUMERIC      NOT NULL,
    --ENUM(debt, credit) COLUMN transaction_type
    transaction_type  STRING(36)   NOT NULL,
    --ENUM(debt, canceled, processing, requires_action, requires_capture, requires_confirmation, requires_payment_method, succeeded) COLUMN status
    status            STRING(36)   NOT NULL,
    payment_method_id STRING(36),
    payment_intent_id STRING(36),
    created_at        TIMESTAMP    NOT NULL,
    updated_at        TIMESTAMP,
    ride_id           STRING(36),
    stripe_id         STRING(64),
) PRIMARY KEY(customer_id, transaction_id),
 INTERLEAVE IN PARENT customers;

CREATE TABLE customer_balance_payment_intents (
    customer_id          STRING(36)   NOT NULL,
    payment_intent_id    STRING(36)   NOT NULL,
    amount               NUMERIC      NOT NULL,
    stripe_id            STRING(64)   NOT NULL,
    stripe_client_secret STRING(64)   NOT NULL,
    --ENUM(canceled, processing, requires_action, requires_capture, requires_confirmation, requires_payment_method, succeeded) COLUMN status
    status               STRING(36)   NOT NULL,
    payment_method_id    STRING(36),
    created_at           TIMESTAMP    NOT NULL,
    updated_at           TIMESTAMP
) PRIMARY KEY(customer_id, payment_intent_id), 
INTERLEAVE IN PARENT customers;

CREATE TABLE customer_devices (
  customer_id STRING(36) NOT NULL,
  device_id STRING(36) NOT NULL,
  os STRING(32),
  version STRING(64),
  model STRING(256),
  app_version STRING(64),
  notifications_token STRING(1024),
  notifications_enabled BOOL,
  location_services_enabled BOOL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  deleted BOOL NOT NULL DEFAULT (FALSE),
  bundle_id STRING(64),
  language_code STRING(8),
  vendor_id STRING(36),
  os_device_id STRING(100),
) PRIMARY KEY(customer_id, device_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

CREATE UNIQUE INDEX idx_customer_devices_customer_id_os_device_id ON customer_devices(customer_id, os_device_id);

CREATE TABLE customer_device_locations (
  customer_id STRING(36) NOT NULL,
  device_id STRING(36) NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  accuracy FLOAT64 NOT NULL,
  updated_at INT64 NOT NULL,
) PRIMARY KEY(customer_id, device_id),
  INTERLEAVE IN PARENT customer_devices ON DELETE CASCADE;

CREATE TABLE customer_estimates (
  customer_id STRING(36) NOT NULL,
  estimate_id STRING(36) NOT NULL,
  service_type_id STRING(36) NOT NULL,
  pick_place_id STRING(36) NOT NULL,
  drop_place_id STRING(36) NOT NULL,
  pickup_at TIMESTAMP NOT NULL,
  distance FLOAT64 NOT NULL,
  duration FLOAT64 NOT NULL,
  route STRING(MAX) NOT NULL,
  total_amount NUMERIC NOT NULL,
  meet_and_greet BOOL NOT NULL,
  pickup_flight_number STRING(64) NOT NULL,
  dropoff_flight_number STRING(64) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  account_id STRING(36),
  pick_place_airport_id STRING(36),
  drop_place_airport_id STRING(36),
) PRIMARY KEY(customer_id, estimate_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

-- Оптимальний індекс для customer_estimates
-- Вирішує N+1 проблему в findCustomerServiceTypesUsage()
-- Покриває агрегації по типах сервісів та хронологічні запити
CREATE INDEX idx_customer_estimates_optimal
ON customer_estimates(customer_id, service_type_id, created_at DESC)
STORING (pickup_at, total_amount, distance, duration);

-- Тестовий дублікат індексу для перевірки продуктивності
-- Ідентичний до основного індексу, використовується для A/B тестування
CREATE INDEX test_idx_customer_estimates_optimal
ON customer_estimates(customer_id, service_type_id, created_at DESC)
STORING (pickup_at, total_amount, distance, duration);

CREATE TABLE customer_estimate_items (
  customer_id STRING(36) NOT NULL,
  estimate_id STRING(36) NOT NULL,
  item_id STRING(36) NOT NULL,
  name STRING(256) NOT NULL,
  amount_formatted STRING(256) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  deleted BOOL,
) PRIMARY KEY(customer_id, estimate_id, item_id),
  INTERLEAVE IN PARENT customer_estimates ON DELETE CASCADE;

CREATE TABLE customer_estimate_rate_items (
  customer_id STRING(36) NOT NULL,
  estimate_id STRING(36) NOT NULL,
  rate_item_id STRING(36) NOT NULL,
  name STRING(128) NOT NULL,
  amount NUMERIC NOT NULL,
  quantity NUMERIC NOT NULL,
  total_amount NUMERIC NOT NULL,
  base BOOL NOT NULL,
  --ENUM(base_fare, per_minute, per_mile, tolls, surge, meet_and_greet, black_car_fund, airport_access_fee, sales_tax, congestion_charge, tip) COLUMN type
  type STRING(32) NOT NULL,
  deleted BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, estimate_id, rate_item_id),
  INTERLEAVE IN PARENT customer_estimates ON DELETE CASCADE;

CREATE TABLE customer_payment_intents (
  customer_id STRING(36) NOT NULL,
  payment_intent_id STRING(36) NOT NULL,
  estimate_id STRING(36) NOT NULL,
  amount NUMERIC NOT NULL,
  stripe_id STRING(64) NOT NULL,
  stripe_client_secret STRING(64) NOT NULL,
  --ENUM(canceled, processing, requires_action, requires_capture, requires_confirmation, requires_payment_method, succeeded) COLUMN status
  status STRING(32) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  payment_method_id STRING(36),
  --ENUM(tip) COLUMN payment_intent_type
  payment_intent_type STRING(36),
) PRIMARY KEY(customer_id, payment_intent_id),
  INTERLEAVE IN PARENT customers ON DELETE NO ACTION;

CREATE TABLE customer_payment_methods (
  customer_id STRING(36) NOT NULL,
  payment_method_id STRING(36) NOT NULL,
  --ENUM(card) COLUMN type
  type STRING(64) NOT NULL,
  stripe_id STRING(128) NOT NULL,
  card_fingerprint STRING(128) NOT NULL,
  card_brand STRING(128) NOT NULL,
  card_holder_name STRING(128) NOT NULL,
  card_last4 STRING(4) NOT NULL,
  card_zip_code STRING(128) NOT NULL,
  default_ BOOL NOT NULL,
  deleted BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  verification_amount1 NUMERIC,
  verification_amount1_stripe_id STRING(64),
  verification_amount2 NUMERIC,
  verification_amount2_stripe_id STRING(64),
  verification_started_at TIMESTAMP,
  verification_expires_at TIMESTAMP,
  verification_verified_at TIMESTAMP,
  verification_attempts INT64,
  --ENUM(not_started, 3ds_started, pending, verified) COLUMN verification_status
  verification_status STRING(32) NOT NULL DEFAULT ('not_started'),
  total_verifications_started INT64,
) PRIMARY KEY(customer_id, payment_method_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

CREATE INDEX customer_payment_methods_by_customer_id_card_fingerprint_idx ON customer_payment_methods(customer_id, card_fingerprint) STORING (type, stripe_id, card_brand, card_holder_name, card_last4, card_zip_code, default_, deleted, updated_at, created_at, verification_amount1, verification_amount1_stripe_id, verification_amount2, verification_amount2_stripe_id, verification_started_at, verification_expires_at, verification_verified_at, verification_attempts, verification_status, total_verifications_started);

CREATE INDEX customer_payment_methods_by_customer_id_idx ON customer_payment_methods(customer_id) STORING (type, stripe_id, card_brand, card_holder_name, card_last4, card_zip_code, default_, deleted, updated_at, created_at, verification_amount1, verification_amount1_stripe_id, verification_amount2, verification_amount2_stripe_id, verification_started_at, verification_expires_at, verification_verified_at, verification_attempts, verification_status, total_verifications_started);

CREATE UNIQUE INDEX customer_payment_methods_stripe_id_uidx ON customer_payment_methods(customer_id, stripe_id);

CREATE TABLE customer_places (
  customer_id STRING(36) NOT NULL,
  place_id STRING(36) NOT NULL,
  address_name STRING(256),
  address STRING(384),
  line_1 STRING(128),
  city STRING(128),
  state STRING(64),
  zipcode INT64,
  lat FLOAT64,
  lon FLOAT64,
  --ENUM( , home, work) COLUMN saved_type
  saved_name STRING(128),
  saved BOOL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  saved_type STRING(32) NOT NULL DEFAULT (""),
  zipcode_str STRING(64),
) PRIMARY KEY(customer_id, place_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

CREATE UNIQUE INDEX customer_places_by_customer_id_lat_lon_idx ON customer_places(customer_id, lat, lon) STORING (address_name, address, line_1, city, state, zipcode, saved_name, saved, updated_at, created_at, saved_type, zipcode_str);

CREATE TABLE customer_place_airports (
  customer_id STRING(36) NOT NULL,
  place_id STRING(36) NOT NULL,
  place_airports_id STRING(36) NOT NULL,
  airport_id STRING(36) NOT NULL,
  airline_id STRING(36),
  terminal_id STRING(36),
  zone_id STRING(36),
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  --ENUM(pick_up, drop_off) COLUMN type
  type STRING(64),
  arrival BOOL,
  departure BOOL,
) PRIMARY KEY(customer_id, place_id, place_airports_id),
  INTERLEAVE IN PARENT customer_places ON DELETE CASCADE;

ALTER TABLE customer_estimates ADD CONSTRAINT FK_customer_estimates_drop_place_airport_id FOREIGN KEY(drop_place_airport_id) REFERENCES customer_place_airports(place_airports_id);

ALTER TABLE customer_estimates ADD CONSTRAINT FK_customer_estimates_pick_place_airport_id FOREIGN KEY(pick_place_airport_id) REFERENCES customer_place_airports(place_airports_id);

CREATE TABLE customer_rides (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  estimate_id STRING(36) NOT NULL,
  payment_method_id STRING(36),
  confirmation_number STRING(256),
  note STRING(512),
  cancellation_reason STRING(512),
  amount NUMERIC NOT NULL,
  amount_charged NUMERIC NOT NULL,
  --ENUM(pending, assigned, en_route, arrived, in_progress, finished, customer_canceled, company_canceled, no_show) COLUMN status
  status STRING(32) NOT NULL,
  --ENUM(customer_account, card, apple_pay, google_pay) COLUMN payment_method_type
  payment_method_type STRING(32) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  first_pick_place_id STRING(36),
  first_drop_place_id STRING(36),
  proxy_call_session_id STRING(36),
  is_offering BOOL,
  --ENUM( , sent, declined) COLUMN review_status
  review_status STRING(32),
  account_id STRING(36),
) PRIMARY KEY(customer_id, ride_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

CREATE INDEX customer_ridesIdx1 ON customer_rides(created_at DESC) STORING (status);

CREATE INDEX customer_rides_by_customer_id_status_idx ON customer_rides(customer_id, status) STORING (estimate_id, payment_method_id, confirmation_number, note, cancellation_reason, amount, amount_charged, payment_method_type, updated_at, created_at, first_pick_place_id, first_drop_place_id, proxy_call_session_id, is_offering);

CREATE INDEX customer_rides_by_status_idx ON customer_rides(status) STORING (estimate_id, payment_method_id, confirmation_number, note, cancellation_reason, amount, amount_charged, payment_method_type, updated_at, created_at, first_pick_place_id, first_drop_place_id, proxy_call_session_id, is_offering);

CREATE TABLE customer_ride_chats (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  created_at TIMESTAMP,
) PRIMARY KEY(customer_id, ride_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_ride_chat_messages (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  message_id STRING(36) NOT NULL,
  entity_id STRING(36) NOT NULL,
  --ENUM(customer, driver) COLUMN entity_type
  entity_type STRING(10),
  message_text STRING(MAX),
  created_at TIMESTAMP,
) PRIMARY KEY(customer_id, ride_id, message_id, entity_id),
  INTERLEAVE IN PARENT customer_ride_chats ON DELETE CASCADE;

CREATE TABLE customer_ride_driver (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  car_id INT64,
  car_make STRING(64),
  car_model STRING(64),
  car_year INT64,
  car_plate STRING(64),
  car_color STRING(64),
  driver_id INT64,
  driver_first_name STRING(64),
  driver_last_name STRING(64),
  driver_phone STRING(64),
  driver_selfie_url STRING(1024),
  driver_selfie_thumb_url STRING(1024),
  driver_rating FLOAT64,
  lat FLOAT64,
  lon FLOAT64,
  heading FLOAT64,
  eta INT64,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  assigned_at TIMESTAMP,
  arrived_at TIMESTAMP,
  picked_up_at TIMESTAMP,
  dropped_off_at TIMESTAMP,
  assigned_at_lat FLOAT64,
  assigned_at_lon FLOAT64,
  arrived_at_lat FLOAT64,
  arrived_at_lon FLOAT64,
  picked_up_at_lat FLOAT64,
  picked_up_at_lon FLOAT64,
  dropped_off_at_lat FLOAT64,
  dropped_off_at_lon FLOAT64,
  car_image STRING(1024),
) PRIMARY KEY(customer_id, ride_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_ride_estimates (
 customer_id     STRING(36)   NOT NULL,
 ride_id         STRING(36)   NOT NULL,
 estimate_id     STRING(36)   NOT NULL,
 created_at      TIMESTAMP    NOT NULL
) PRIMARY KEY(customer_id, ride_id, estimate_id),
INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_ride_guests (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  guest_id STRING(36) NOT NULL,
  token STRING(32) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, ride_id, guest_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE INDEX idx_customer_ride_guests_guest_id ON customer_ride_guests(guest_id);

CREATE INDEX idx_customer_ride_guests_token ON customer_ride_guests(token);

CREATE TABLE customer_ride_live_activities (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  device_id STRING(36) NOT NULL,
  token STRING(1024) NOT NULL,
  --ENUM(active, inactive) COLUMN status
  status STRING(32) NOT NULL,
  last_sent_at TIMESTAMP,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, ride_id, device_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE NO ACTION;

CREATE INDEX customer_ride_live_activities_by_customer_id_ride_id_status_idx ON customer_ride_live_activities(customer_id, ride_id, status) STORING (token, last_sent_at, updated_at, created_at);

CREATE TABLE customer_ride_notifications (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  device_id STRING(36) NOT NULL,
  jurny_driver_id INT64 NOT NULL,
  --ENUM(driver_was_redispatched, driver_on_way, driver_almost_on_point, driver_arrived, almost_there, trip_is_finished, trip_is_finished_review, driver_refused, driver_message) COLUMN notification_id
  notification_id STRING(36) NOT NULL,
  token STRING(256) NOT NULL,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, ride_id, device_id, jurny_driver_id, notification_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_ride_receipt (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  receipt_id STRING(36) NOT NULL,
  file_id STRING(36) NOT NULL,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, ride_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_ride_transactions (
  customer_id STRING(36) NOT NULL,
  ride_id STRING(36) NOT NULL,
  transaction_id STRING(36) NOT NULL,
  payment_method_id STRING(36),
  amount NUMERIC NOT NULL,
  stripe_id STRING(64),
  --ENUM(canceled, processing, requires_action, requires_capture, requires_confirmation, requires_payment_method, succeeded) COLUMN status
  status STRING(32) NOT NULL,
  attempts INT64 NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  payment_intent_id STRING(36),
) PRIMARY KEY(customer_id, ride_id, transaction_id),
  INTERLEAVE IN PARENT customer_rides ON DELETE CASCADE;

CREATE TABLE customer_verifications (
  customer_id STRING(36) NOT NULL,
  verification_id STRING(32) NOT NULL,
  status STRING(32) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, verification_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE;

CREATE TABLE customer_verification_files (
  customer_id STRING(36) NOT NULL,
  --ENUM(state_issued_id, driver_license, card) COLUMN verification_id
  verification_id STRING(32) NOT NULL,
  file_id STRING(36) NOT NULL,
  --ENUM(pending, verified, rejected) COLUMN type
  type STRING(32) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(customer_id, verification_id, file_id),
  INTERLEAVE IN PARENT customer_verifications ON DELETE CASCADE;

CREATE TABLE files (
  file_id STRING(36) NOT NULL,
  name STRING(512) NOT NULL,
  path STRING(512) NOT NULL,
  extension STRING(64) NOT NULL,
  deleted BOOL,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(file_id);

ALTER TABLE customer_verification_files ADD FOREIGN KEY(file_id) REFERENCES files(file_id);

CREATE TABLE register_tokens (
  phone STRING(20) NOT NULL,
  token STRING(1024) NOT NULL,
  expires_at TIMESTAMP NOT NULL,
) PRIMARY KEY(phone, token);

CREATE TABLE sales_tax_rates (
  sales_tax_rate_id STRING(36) NOT NULL,
  county_name STRING(100) NOT NULL,
  city_name STRING(100),
  tax_rate NUMERIC NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP,
) PRIMARY KEY(sales_tax_rate_id);

CREATE TABLE service_area_s2cells (
  service_area_id STRING(36) NOT NULL,
  s2cell_id INT64 NOT NULL,
) PRIMARY KEY(service_area_id, s2cell_id);

CREATE TABLE service_areas (
  service_area_id STRING(36) NOT NULL,
  name STRING(255),
  geo_json JSON,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(service_area_id);

CREATE TABLE service_rates (
  service_rate_id STRING(36) NOT NULL,
  name STRING(100) NOT NULL,
  --ENUM(meet_and_greet, black_car_fund, airport_access_fee, congestion_charge) COLUMN type
  alias STRING(100) NOT NULL,
  --ENUM(percent, fixed) COLUMN value_type
  value_type STRING(20) NOT NULL,
  value NUMERIC NOT NULL,
  description STRING(MAX),
  --ENUM(active, inactive, deleted) COLUMN status
  status STRING(32) NOT NULL,
  calculation_method STRING(100),
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(service_rate_id);

CREATE TABLE service_types (
  service_type_id STRING(36) NOT NULL,
  --ENUM(regular, comfort, premium, share, xl, premium_xl, electric, accessible, minivan) COLUMN code
  code STRING(64) NOT NULL,
  name STRING(64) NOT NULL,
  description STRING(128) NOT NULL,
  min_amount NUMERIC NOT NULL,
  deleted BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  seats INT64,
) PRIMARY KEY(service_type_id);

CREATE UNIQUE INDEX service_types_code_uidx ON service_types(code);

CREATE TABLE service_type_rates (
  service_type_id STRING(36) NOT NULL,
  rate_id STRING(36) NOT NULL,
  name STRING(64) NOT NULL,
  description STRING(128) NOT NULL,
  amount NUMERIC NOT NULL,
  --ENUM(base, per_minute, per_mile) COLUMN type
  type STRING(32) NOT NULL,
  deleted BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(service_type_id, rate_id),
  INTERLEAVE IN PARENT service_types ON DELETE CASCADE;

CREATE TABLE sessions (
  session_id STRING(36) NOT NULL,
  customer_phone STRING(15) NOT NULL,
  driver_phone STRING(15) NOT NULL,
  proxy_number STRING(50) NOT NULL,
  ttl INT64 NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  deleted BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  reference_id STRING(256),
) PRIMARY KEY(session_id);

CREATE TABLE session_calls (
  session_id STRING(36) NOT NULL,
  call_id STRING(36) NOT NULL,
  caller_phone STRING(15) NOT NULL,
  callee_phone STRING(15) NOT NULL,
  proxy_number STRING(15) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(session_id, call_id),
  INTERLEAVE IN PARENT sessions ON DELETE NO ACTION;

CREATE TABLE tollway_validations (
  tollway_validation_id STRING(36) NOT NULL,
  pu_region STRING(50),
  do_region STRING(50),
  toll_name STRING(MAX),
  active_toll STRING(MAX),
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(tollway_validation_id);

CREATE TABLE tollways (
  tollway_id STRING(36) NOT NULL,
  name STRING(128) NOT NULL,
  name_key STRING(128),
  type STRING(32) NOT NULL,
  amount NUMERIC NOT NULL,
  polygon JSON NOT NULL,
  deleted BOOL NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(tollway_id);

CREATE TABLE webhooks (
  webhook_id STRING(36) NOT NULL,
  name STRING(32) NOT NULL,
  type STRING(32) NOT NULL,
  request_url STRING(256) NOT NULL,
  request_method STRING(16) NOT NULL,
  request_headers JSON NOT NULL,
  request_body JSON NOT NULL,
  request_dump STRING(MAX) NOT NULL,
  response_time INT64 NOT NULL,
  response_status_code INT64 NOT NULL,
  response_headers JSON NOT NULL,
  response_body JSON NOT NULL,
  response_dump STRING(MAX) NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(webhook_id);

CREATE INDEX webhooksIdx1 ON webhooks(request_method, created_at DESC) STORING (name, request_body, request_dump, request_headers, request_url, response_body, response_dump, response_headers, response_status_code, response_time, type, updated_at);

CREATE TABLE zip_codes (
  code STRING(16) NOT NULL,
  lat FLOAT64 NOT NULL,
  lon FLOAT64 NOT NULL,
  state STRING(32) NOT NULL,
  city STRING(32) NOT NULL,
  county STRING(32) NOT NULL,
) PRIMARY KEY(code);

CREATE TABLE verification_codes (
  verification_code_id STRING(36) NOT NULL,
  email STRING(36) NOT NULL,
  code STRING(36) NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY(verification_code_id, email);

CREATE TABLE blocked_devices (
  os_device_id STRING(100) NOT NULL,
  reason STRING(256),
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(os_device_id);;

CREATE TABLE areas (
                       area_id     STRING(36) NOT NULL,
                       name        STRING(128),
                       address     STRING(256),
                       geojson     JSON NOT NULL,
                       quantity    INT64 NOT NULL,
                       updated_at  TIMESTAMP,
                       created_at  TIMESTAMP NOT NULL
) PRIMARY KEY(area_id);

CREATE TABLE area_points (
                             area_point_id STRING(36) NOT NULL,
                             area_id       STRING(36) NOT NULL,
                             name          STRING(128),
                             address       STRING(256),
                             lat           FLOAT64 NOT NULL,
                             lon           FLOAT64 NOT NULL,
                             --ENUM(pick_up, drop_off, both) COLUMN type
                             type          STRING(36) NOT NULL,
                             updated_at    TIMESTAMP,
                             created_at    TIMESTAMP NOT NULL
) PRIMARY KEY(area_id, area_point_id),
    INTERLEAVE IN PARENT areas ON DELETE CASCADE;

CREATE INDEX idx_area_points_type
    ON area_points (type)
    STORING (name, address, lat, lon);

CREATE INDEX idx_area_points_lat_lon
    ON area_points (lat, lon)
    STORING (type, name, address);

CREATE TABLE app_version (
  app_version_id   STRING(36) NOT NULL,
  --ENUM(android, ios) COLUMN os
  os               STRING(12) NOT NULL,        
  required_version STRING(32) NOT NULL,        
  current_version  STRING(32) NOT NULL,        
  updated_at       TIMESTAMP,                   
  created_at       TIMESTAMP NOT NULL          
) PRIMARY KEY (app_version_id);