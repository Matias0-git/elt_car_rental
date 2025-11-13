b.​ Los 5 estados con menor cantidad de alquileres (mostrar query y visualización)

SELECT state, COUNT(Id) AS total_alquileres
FROM car_rental_analytics
WHERE state IS NOT NULL
GROUP BY state
ORDER BY total_alquileres ASC
LIMIT 5;

![alt text](ej2b.png)
