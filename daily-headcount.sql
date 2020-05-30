WITH sfrstca_audit AS (
        SELECT aa.sfrstca_term_code,
               aa.sfrstca_pidm,
               aa.sfrstca_crn,
               bb.stvrsts_incl_sect_enrl,
               TRUNC(aa.sfrstca_rsts_date) AS sfrstca_rsts_date,
               ROW_NUMBER() OVER (PARTITION BY aa.sfrstca_term_code, aa.sfrstca_pidm, aa.sfrstca_crn, TRUNC(aa.sfrstca_rsts_date)
                                      ORDER BY aa.sfrstca_seq_number DESC) AS sfrstca_rn,
               CASE WHEN MAX(aa.sfrstca_seq_number) OVER (PARTITION BY aa.sfrstca_term_code, aa.sfrstca_pidm, aa.sfrstca_crn
                                                              ORDER BY TRUNC(aa.sfrstca_rsts_date)
                                                               ROWS UNBOUNDED PRECEDING) = aa.sfrstca_seq_number
                    THEN 'Y' ELSE 'N'
                    END AS max_eff_seq
          FROM sfrstca aa
    INNER JOIN stvrsts bb
            ON aa.sfrstca_rsts_code = bb.stvrsts_code
    INNER JOIN sfrstcr cc
            ON cc.sfrstcr_term_code = aa.sfrstca_term_code
           AND cc.sfrstcr_pidm = aa.sfrstca_pidm
           AND cc.sfrstcr_crn = aa.sfrstca_crn
         WHERE aa.sfrstca_source_cde = 'BASE'
)

    SELECT a.sfrstca_term_code,
           a.sfrstca_pidm,
           a.sfrstca_crn,
           a.stvrsts_incl_sect_enrl,
           a.sfrstca_rsts_date AS record_begin_date,
           COALESCE(
              LAG(a.sfrstca_rsts_date) OVER
                  (PARTITION BY a.sfrstca_term_code, a.sfrstca_pidm, a.sfrstca_crn
                       ORDER BY a.sfrstca_rsts_date DESC),
              b.stvterm_end_date) AS record_end_date
      FROM sfrstca_audit a
INNER JOIN stvterm b
        ON a.sfrstca_term_code = b.stvterm_code
INNER JOIN sfrrsts c
        ON b.stvterm_code = c.sfrrsts_term_code
       AND c.sfrrsts_ptrm_code = '1'
       AND c.sfrrsts_rsts_code = 'RW'
       AND c.sfrrsts_start_date <= SYSDATE -- Only load terms for which registration is open (or has occurred).
     WHERE a.sfrstca_rn = 1
       AND a.max_eff_seq = 'Y'
       AND b.stvterm_code > '201530'

