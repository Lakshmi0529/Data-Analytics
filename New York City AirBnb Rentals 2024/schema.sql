CREATE TABLE listings_2024 (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood_group NVARCHAR(100),
    neighbourhood NVARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(100),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
	rating FLOAT
);

CREATE TABLE Brooklyn (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood NVARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(100),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
	rating FLOAT
);

CREATE TABLE Bronx (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood NVARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(100),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
	rating FLOAT
);

CREATE TABLE Manhattan (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood NVARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(100),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
	rating FLOAT
);

CREATE TABLE Staten_Island (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood NVARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(100),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
	rating FLOAT
);

CREATE TABLE Queens (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood NVARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(100),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
	rating FLOAT
);

-- Inserting data into the above tables for different Boroughs (Brooklyn, Bronx, Manhattan, Queens, Staten Island)

INSERT INTO Brooklyn (id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating)
SELECT id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating
FROM listings_2024
WHERE neighbourhood_group = 'Brooklyn'

INSERT INTO Bronx(id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,
         number_of_reviews,last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating)
SELECT id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating
FROM listings_2024
WHERE neighbourhood_group = 'Bronx'

INSERT INTO Manhattan(id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating)
SELECT id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating
FROM listings_2024
WHERE neighbourhood_group = 'Manhattan'

INSERT INTO Queens(id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating)
SELECT id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating
FROM listings_2024
WHERE neighbourhood_group = 'Queens'

INSERT INTO Staten_Island (id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating)
SELECT id , name,host_id,host_name,neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,
			last_review,reviews_per_month,calculated_host_listings_count,availability_365,rating
FROM listings_2024
WHERE neighbourhood_group = 'Staten Island'


-- Creating a table to analyze host data 

CREATE TABLE Hosts (
		     host_id INT PRIMARY KEY,
			 host_name NVARCHAR(255),
			 host_listing_count INT,
			 host_total_reviews INT
			 );

INSERT INTO Hosts (host_id,host_name,host_listing_count,host_total_reviews)
SELECT host_id, host_name,calculated_host_listings_count,SUM(number_of_reviews)
FROM listings_2024
GROUP BY host_id, host_name,calculated_host_listings_count

-- For analyzing Neighbourhoods within a particular Borough

CREATE TABLE Neighbourhoods (
		neighbourhood NVARCHAR(100),
		average_price FLOAT,
		number_of_reviews INT
);

INSERT INTO Neighbourhoods( neighbourhood,average_price,number_of_reviews)
SELECT neighbourhood,AVG(price),SUM(number_of_reviews)
FROM listings_2024
GROUP BY neighbourhood

-- 