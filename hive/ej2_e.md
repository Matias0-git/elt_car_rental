e.​ las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o
electrico)

SELECT city, COUNT(Id) AS total_ecologicos
FROM car_rental_analytics
WHERE (UPPER(fuel) = 'HYBRID' OR UPPER(fuel) = 'ELECTRIC')
  AND city IS NOT NULL
GROUP BY city
ORDER BY total_ecologicos DESC
LIMIT 5;

![alt text](ej2_e.png)

