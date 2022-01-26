DROP TABLE IF EXISTS pets;
CREATE TABLE pets (id SERIAL PRIMARY KEY, name VARCHAR(100), species VARCHAR(100), birth DATE);
INSERT INTO pets (name, species, birth) VALUES ('Polly', 'Parrot', '2020-01-01');

INSERT INTO pets (name, species, birth)
VALUES
       ('Ginger', 'Dog', '2015-05-09'),
       ('Spice', 'Dog', '2015-05-09');
