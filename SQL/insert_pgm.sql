INSERT INTO PGM (
    PID
    ,CD
    ,ESD
    ,PSD
    ,YP
    ,HR
    ,sq
    ,del
    ,dts
    ) VALUES (
    :id
    ,:pgm_code
    ,:start_date
    ,:pgm_start_date
    ,0
    ,0
    ,:sq
    ,0
    ,GETDATE()
    )