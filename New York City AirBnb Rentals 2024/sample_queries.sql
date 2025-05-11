-- Borough level summary
SELECT 
	neighbourhood_group,
	COUNT(*) AS total_listings,
	ROUND(AVG(price),2) AS avg_price,
	ROUND(AVG(rating),2) AS avg_rating,
	SUM(number_of_reviews) AS total_reviews
FROM listings_2024
GROUP BY neighbourhood_group
ORDER BY total_listings DESC

-- Price Quartiles by Borough and Room type

-- Calculating percentiles separately then join
WITH StatsBase AS (
    SELECT 
        neighbourhood_group,
        room_type,
        price,
        COUNT(*) OVER (PARTITION BY neighbourhood_group, room_type) AS listings_count
    FROM listings_2024
),
Percentiles AS (
    SELECT DISTINCT
        neighbourhood_group,
        room_type,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) OVER (PARTITION BY neighbourhood_group, room_type) AS price_25pctl,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) OVER (PARTITION BY neighbourhood_group, room_type) AS median_price,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) OVER (PARTITION BY neighbourhood_group, room_type) AS price_75pctl
    FROM StatsBase
),
Aggregates AS (
    SELECT 
        neighbourhood_group,
        room_type,
        COUNT(*) AS listings_count,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price
    FROM listings_2024
    GROUP BY neighbourhood_group, room_type
)

SELECT 
    a.neighbourhood_group,
    a.room_type,
    a.listings_count,
    a.min_price,
    a.max_price,
    a.avg_price,
    p.price_25pctl,
    p.median_price,
    p.price_75pctl
FROM Aggregates a
JOIN Percentiles p ON a.neighbourhood_group = p.neighbourhood_group 
                   AND a.room_type = p.room_type
ORDER BY a.neighbourhood_group, a.room_type;


-- Neighbourhood Performance

SELECT 
	 neighbourhood,
	 ROUND(AVG(price),2) AS avg_price,
	 SUM(number_of_reviews) AS total_reviews
FROM listings_2024
GROUP BY neighbourhood
ORDER BY avg_price DESC

-- Host Performance Analysis
--- Top performing hosts by number of listings and reviews
SELECT
	  host_id,
	  host_name,
	  host_listing_count,
	  host_total_reviews,
	  host_total_reviews/host_listing_count as avg_reviews_per_listing  
FROM Hosts
ORDER BY host_total_reviews DESC
OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY

-- Detailed Neighbourhood analysis within a Borough
SELECT
	 neighbourhood,
	 COUNT(*) AS listings_count,
	 AVG(price) as avg_price,
	 ROUND(AVG(rating),2) as avg_rating,
	 SUM(number_of_reviews) as total_reviews,
	 AVG(availability_365) as avg_availability
FROM Brooklyn
GROUP BY neighbourhood
HAVING COUNT(*) > 10
ORDER BY avg_price DESC

-- Room type Popularity across all Boroughs

SELECT 
	  neighbourhood_group,
	  room_type,
	  COUNT(*) AS listings_count,
	  COUNT(*)*100/SUM(COUNT(*)) OVER (PARTITION BY neighbourhood_group) AS percentage,
	  AVG(price) AS avg_price,
	  ROUND(AVG(rating),2) AS avg_rating
FROM listings_2024
GROUP BY neighbourhood_group,room_type
ORDER BY neighbourhood_group,listings_count DESC

-- Monthly Review Activity Trends

SELECT 
	  neighbourhood_group,
	  DATEPART(YEAR,last_review) AS review_year,
	  DATEPART(MONTH,last_review) AS review_month,
	  COUNT(*) AS review_count
FROM listings_2024
WHERE last_review IS NOT NULL
GROUP BY neighbourhood_group,DATEPART(YEAR,last_review),DATEPART(MONTH,last_review)
ORDER BY review_year,review_month,neighbourhood_group

-- Availability Analysis

WITH Availability_buckets AS (
	SELECT 
		neighbourhood_group,
		price,
		rating,
		CASE
			WHEN availability_365 = 0 THEN '0 (Not Available)'
			WHEN availability_365 BETWEEN 1 AND 30 THEN '1-30 days'
            WHEN availability_365 BETWEEN 31 AND 90 THEN '31-90 days'
            WHEN availability_365 BETWEEN 91 AND 180 THEN '91-180 days'
            ELSE '181+ days'
        END AS availability_bucket
    FROM listings_2024
)
SELECT 
	  neighbourhood_group,
	  availability_bucket,
	  COUNT(*) AS listings_count,
	  ROUND(AVG(price),2) AS avg_price,
	  ROUND(AVG(rating),2) AS avg_rating
FROM Availability_buckets
GROUP BY neighbourhood_group,availability_bucket
ORDER BY neighbourhood_group,availability_bucket

-- Data for creating a Geographic Heatmap

-- Data for geographic visualization in Tableau
SELECT 
    id,
    name,
    neighbourhood_group,
    neighbourhood,
    latitude,
    longitude,
    price,
    rating,
    room_type,
    availability_365
FROM listings_2024
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Correlation Analysis ( How different factors correlate with price)

SELECT 
	 neighbourhood_group,
	 room_type,
	 AVG(price) AS avg_price,
	 ROUND(AVG(rating),2) AS avg_rating,
	 AVG(number_of_reviews) AS avg_reviews,
	 ROUND(AVG(reviews_per_month),2) AS avg_reviews_per_month,
	 AVG(calculated_host_listings_count) AS avg_host_listings
FROM listings_2024
GROUP BY neighbourhood_group, room_type
ORDER BY neighbourhood_group, room_type

-- Identifying potential Hosts with superior performance metrics
SELECT 
	 h.host_id,
	 h.host_name,
	 h.host_listing_count,
	 h.host_total_reviews,
	 ROUND(AVG(l.rating),2) AS avg_rating_across_listings,
	 MIN(l.rating) AS min_rating,
	 COUNT(DISTINCT l.neighbourhood_group) AS boroughs_covered
FROM Hosts h
JOIN listings_2024 l ON h.host_id = l.host_id
GROUP BY h.host_id, h.host_name, h.host_listing_count, h.host_total_reviews
HAVING AVG(l.rating) >= 4.8 AND MIN(l.rating) >= 4.5 AND h.host_listing_count >=2
ORDER BY h.host_total_reviews DESC