SELECT COUNT(DISTINCT x.sfrstca_pidm)
  FROM (
       SELECT a.sfrstca_pidm,
              a.sfrstca_term_code,
              a.sfrstca_crn,
              b.stvrsts_desc,
              b.stvrsts_incl_sect_enrl
         FROM sfrstca a
    LEFT JOIN stvrsts b
           ON a.sfrstca_rsts_code = b.stvrsts_code
    LEFT JOIN ssbsect c
           ON a.sfrstca_term_code = c.ssbsect_term_code
          AND a.sfrstca_crn = c.ssbsect_crn
   INNER JOIN sfrstcr d
           ON a.sfrstca_term_code = d.sfrstcr_term_code
          AND a.sfrstca_pidm = d.sfrstcr_pidm
          AND a.sfrstca_crn = d.sfrstcr_crn
        WHERE a.sfrstca_term_code = '201740'
          AND a.sfrstca_source_cde = 'BASE'
          AND c.ssbsect_ssts_code = 'A'
          AND NVL(c.ssbsect_credit_hrs, 1) <> 0
          AND a.sfrstca_rsts_date = (SELECT MAX(aa.sfrstca_rsts_date)
                                           FROM sfrstca aa
                                          WHERE a.sfrstca_pidm = aa.sfrstca_pidm
                                            AND a.sfrstca_crn = aa.sfrstca_crn
                                            AND aa.sfrstca_rsts_date <= '11-SEP-2017')
          AND a.sfrstca_seq_number = (SELECT MAX(aaa.sfrstca_seq_number)
                                        FROM sfrstca aaa
                                       WHERE a.sfrstca_pidm = aaa.sfrstca_pidm
                                         AND a.sfrstca_crn = aaa.sfrstca_crn
                                         AND aaa.sfrstca_rsts_date = a.sfrstca_rsts_date)
                                        ) x
    WHERE (x.stvrsts_incl_sect_enrl = 'Y')
