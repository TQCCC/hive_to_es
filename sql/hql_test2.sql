SELECT
  `id`,
  `name`,
  `age`
FROM student
  LEFT JOIN score ON score.id = student.id
WHERE score.math_score > 90 AND score.english_score > 90