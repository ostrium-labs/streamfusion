INSERT INTO sink SELECT r.`record_id`, r.`user_id`, u.`id` FROM Records r INNER JOIN Users u ON u.`user_id` = r.`user_id` AND u.`id` = r.`record_id` WHERE r.`user_id` IN (1, 2);
