INSERT INTO PGM (
    PID
    ,CD
    ,ESD
    ,YP
    ,HR
    ,sq
    ,del
    ,dts
    ) VALUES (
    :id
    ,:pgm_code
    ,:start_date
    ,0
    ,0
    ,:sq
    ,0
    ,NOW()
    )