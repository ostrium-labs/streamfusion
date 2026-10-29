INSERT INTO sink WITH
    section_detail as (
        SELECT s.event_section_id
        
        FROM upsert_table_with_duplicates s
    ),

    event_element as (
        SELECT
            ed.id as event_element_id
        FROM (
          SELECT
                 'pk-2' id,
                 'section_id_3' section_id
           UNION ALL
          SELECT
                 'pk-1' id,
                 'section_id_3' section_id
        ) ed  
        LEFT JOIN
            section_detail as s
            ON s.event_section_id = ed.section_id
    )

SELECT  event_element_id, COUNT(*) cnt
FROM event_element
GROUP BY event_element_id;
