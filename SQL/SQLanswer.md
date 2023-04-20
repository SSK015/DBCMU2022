<!-->wait to download files<!-->
**q2** 
SELECT primary_title, premiered, runtime_minutes, '(mins)' 
FROM titles
ORDER BY runtime_minutes DESC
LIMIT 10;
**q3**
SELECT name, 2022 - born
FROM people
WHERE born IS NOT NULL
AND BORN > 1900
AND died IS NULL
ORDER BY born
LIMIT 20;
**q4**
SELECT people.name, COUNT(crew.person_id)
FROM crew, people
WHERE crew.person_id = people.person_id
GROUP BY people.name
ORDER BY COUNT(crew.person_id) DESC
LIMIT 20;
**q5**
SELECT CAST(titles.premiered / 10 AS INTEGER) * 10 AS decade, AVG(rating), MAX(rating), MIN(rating), COUNT(rating)
FROM titles
JOIN ratings
WHERE premiered IS NOT NULL
AND premiered >= premiered - premiered % 10
AND premiered < premiered - premiered % 10 + 10
AND titles.title_id = ratings.title_id
GROUP BY decade
ORDER BY AVG(rating) DESC, decade ASC;
