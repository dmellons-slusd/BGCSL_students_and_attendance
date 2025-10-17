select top 1 sq
from pgm 
where 
pid = :id
order by sq desc