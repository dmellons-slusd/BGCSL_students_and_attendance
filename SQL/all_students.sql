WITH all_stu AS (
    SELECT 
    stu.ID, STU.LN, STU.FN, CONVERT(DATE, STU.BD) BD
    , stu.SC, loc.NM, stu.GR
    , stu.AD, stu.CY, stu.ST, stu.ZC
    , stu.TL
    , stu.CL
    , stu.PG, stu.PEM, stu.SEM
    , stu.SN
    , stu.MN, stu.GN
    , stu.FNA, stu.LNA
    , CASE
        WHEN STU.GR >= 6 THEN COUN.TE
        ELSE ''
    END AS counselor
    , stu.U11
    , EL.DE as ELL
    , stu.GG, stu.GGN
    , stu.TG
    , stu.LD
    , stu.LF
    , stu.SP, stu.CID
    , RACE.DE as resolved_race
    , HOME.DE as home_lang
    , CON.DE as con_lang
    , ROW_NUMBER() OVER (PARTITION BY STU.ID 
        ORDER BY STU.DEL
        , CASE WHEN stu.LD IS NULL THEN 0 ELSE 1 END ASC
        , CASE WHEN stu.ED IS NULL THEN 0 ELSE 1 END DESC
        , stu.ED ASC
        , stu.TG
        , stu.SC
    ) AS RN
    FROM STU
    LEFT JOIN LOC ON stu.SC = loc.CD
    LEFT JOIN TCH AS COUN ON STU.CU = COUN.TN AND STU.SC=COUN.SC
    LEFT JOIN COD EL ON EL.TC='STU' AND EL.FC='LF' AND EL.CD = STU.LF
    left outer join SUP ON STU.SC = SUP.SC AND STU.SN = SUP.SN
    Left outer JOIN COD RACE ON RACE.TC = 'STU' AND RACE.FC = 'EC' AND SUP.ARD = RACE.CD
    LEFT JOIN COD HOME ON HOME.TC = 'STU' AND HOME.FC = 'HL' AND STU.HL = HOME.CD
    LEFT JOIN COD CON ON CON.TC = 'STU' AND CON.FC = 'HL' AND STU.CL = CON.CD
    WHERE 
    1=1
    AND [STU].DEL = 0
    AND stu.gr not in('14', '17', '18')
    AND stu.sp not in('K', 'N')
)
SELECT 
SC, NM, ID, LN, FN, GR, BD
, PEM, SEM
, AD, CY, ST, ZC
FROM all_stu
WHERE RN = 1