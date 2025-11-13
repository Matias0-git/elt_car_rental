a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos
ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.

SELECT COUNT(Id) AS total_alquileres_eco
FROM car_rental_analytics
WHERE (UPPER(fuel) = 'HYBRID' OR UPPER(fuel) = 'ELECTRIC')
  AND rating >= 4;

![alt text](ej25.png)
