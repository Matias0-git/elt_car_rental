f.​ el promedio de reviews, segmentando por tipo de combustible

SELECT fuel, AVG(rating) AS promedio_rating
FROM car_rental_analytics
WHERE fuel IS NOT NULL
GROUP BY fuel
ORDER BY promedio_rating DESC;

![alt text](ej2f.png)
