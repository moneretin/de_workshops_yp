CREATE OR REPLACE VIEW blue_cars AS
SELECT id, license_plate FROM cars WHERE color='blue';