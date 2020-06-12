WITH sfrstca_audit AS (
    SELECT a.sfrstca_term_code,
           a.sfrstca_pidm,
           a.sfrstca_crn,
           d.stvrsts_incl_sect_enrl,
           TRUNC(a.sfrstca_rsts_date) AS sfrstca_rsts_date,
           ROW_NUMBER() OVER (PARTITION BY a.sfrstca_term_code, a.sfrstca_pidm, a.sfrstca_crn, TRUNC(a.sfrstca_rsts_date)
                                  ORDER BY a.sfrstca_seq_number DESC) AS sfrstca_rn,
           CASE WHEN MAX(a.sfrstca_seq_number) OVER (PARTITION BY a.sfrstca_term_code, a.sfrstca_pidm, a.sfrstca_crn
                                          ORDER BY TRUNC(a.sfrstca_rsts_date)
                                         ROWS UNBOUNDED PRECEDING) = a.sfrstca_seq_number THEN 'Y' ELSE 'N' END   AS max_eff_seq

      FROM sfrstca a
    INNER JOIN stvrsts d
       ON a.sfrstca_rsts_code = d.stvrsts_code
         INNER JOIN sfrstcr f
                 ON f.sfrstcr_term_code = a.sfrstca_term_code
                AND f.sfrstcr_pidm = a.sfrstca_pidm
                AND f.sfrstcr_crn = a.sfrstca_crn
       AND a.sfrstca_source_cde = 'BASE'
)


SELECT b.sfrstca_term_code,
       b.sfrstca_pidm,
       b.sfrstca_crn,
       b.stvrsts_incl_sect_enrl,
       b.sfrstca_rsts_date AS record_begin_date,
       COALESCE(
        LAG(b.sfrstca_rsts_date) OVER (PARTITION BY b.sfrstca_term_code, b.sfrstca_pidm, b.sfrstca_crn ORDER BY b.sfrstca_rsts_date DESC),
        c.stvterm_end_date) AS record_end_date
FROM sfrstca_audit b
LEFT JOIN stvterm c
      ON b.sfrstca_term_code = c.stvterm_code
   WHERE b.sfrstca_rn = 1
     AND b.max_eff_seq = 'Y'
     AND b.sfrstca_term_code > '201530'