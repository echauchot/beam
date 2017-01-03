create KEYSPACE IF NOT EXISTS beam WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
use  beam;
CREATE TABLE IF NOT EXISTS  test ( 
	name text, 
	PRIMARY KEY (name) 
);

INSERT INTO test (name) VALUES ('Einstein');
INSERT INTO test (name) VALUES ('Darwin');
INSERT INTO test (name) VALUES ('Copernicus');
INSERT INTO test (name) VALUES ('Pasteur');
INSERT INTO test (name) VALUES ('Curie');
INSERT INTO test (name) VALUES ('Faraday');
INSERT INTO test (name) VALUES ('Newton');
INSERT INTO test (name) VALUES ('Bohr');
INSERT INTO test (name) VALUES ('Galilei');
INSERT INTO test (name) VALUES ('Maxwell');

