select *
from pgm 
where pid = :id 
and cd = :pgm_code
and eed is null