SELECT f.stvterm_code,
       f.full_date,
       f.days_before_start,
       COUNT(DISTINCT f.sfrstca_pidm)
FROM (
         SELECT a.stvterm_code,
                c.full_date,
                (c.full_date - a.stvterm_start_date) AS days_before_start,
                d.sfrstca_pidm,
                d.sfrstca_crn,
                d.sfrstca_rsts_code,
                ROW_NUMBER() OVER (PARTITION BY a.stvterm_code, c.full_date, d.sfrstca_pidm, d.sfrstca_crn
                                       ORDER BY d.sfrstca_seq_number DESC) AS rn
           FROM stvterm a
     INNER JOIN sfrrsts b -- Opening date for web registration (SFRSTS_START_DATE).
             ON a.stvterm_code = b.sfrrsts_term_code
            AND b.sfrrsts_rsts_code = 'RW'
            AND b.sfrrsts_ptrm_code = '1'
      LEFT JOIN d_date@DSCIR c
             ON c.full_date >= b.sfrrsts_start_date
            AND c.full_date <= a.stvterm_end_date
      LEFT JOIN sfrstca d
             ON d.sfrstca_term_code = a.stvterm_code
            AND d.sfrstca_rsts_date <= c.full_date
            AND d.sfrstca_source_cde = 'BASE'
     INNER JOIN sfrstcr f
             ON f.sfrstcr_term_code = d.sfrstca_term_code
            AND f.sfrstcr_pidm = d.sfrstca_pidm
            AND f.sfrstcr_crn = d.sfrstca_crn
          WHERE d.sfrstca_term_code IN ('201840','201940','202040')
            AND c.full_date <= SYSDATE
    ) f
WHERE f.rn = 1
  AND f.sfrstca_rsts_code IN (SELECT stvrsts_code FROM stvrsts WHERE stvrsts_incl_sect_enrl = 'Y')
GROUP BY stvterm_code, full_date, days_before_start
