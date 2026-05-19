-- Source SHA-256: 407f863469b0d8d497d4b2021cb5a605d88ddf9fe5946f20a4f5a6a901ae0971

UPDATE shield_documents
SET title = 'Violence weapons bomb shooting murder homicide kill stabbing assault weapon making explosives explosive bomb attack самодельное оружие взрывчатка изготовление бомбы зарезать убить напасть оружие нападение',
    embedding = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE slug = 'violent_harm_weapons'
  AND title IS DISTINCT FROM 'Violence weapons bomb shooting murder homicide kill stabbing assault weapon making explosives explosive bomb attack самодельное оружие взрывчатка изготовление бомбы зарезать убить напасть оружие нападение';
