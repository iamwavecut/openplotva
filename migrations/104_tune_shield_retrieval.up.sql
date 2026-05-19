-- Source SHA-256: 2a2b6cc2f8b34c37df61fc653403e3dd41cd66b8345956ab09ae5c7bd2121f53

DROP INDEX IF EXISTS shield_documents_title_search_idx;
ALTER TABLE shield_documents DROP COLUMN IF EXISTS title_search;
ALTER TABLE shield_documents
    ADD COLUMN title_search tsvector GENERATED ALWAYS AS (to_tsvector('russian', title)) STORED;
CREATE INDEX IF NOT EXISTS shield_documents_title_search_idx
    ON shield_documents USING gin (title_search);

UPDATE shield_documents AS d
SET title = v.title,
    embedding = NULL,
    updated_at = CURRENT_TIMESTAMP
FROM (
    VALUES
    ('self_harm_crisis', 'Self-harm suicide suicidal ideation wanting to die I want to die I do not want to live hurt myself cut myself overdose pills tablets jump hang kill myself самоубийство суицид хочу умереть не хочу жить покончить с собой причинить себе вред порезать себя таблетки выпить таблетки прыгнуть повеситься'),
    ('psychosis_delusion_deescalation', 'Psychosis delusion paranoia voices mania reality break mind control gang stalking surveillance secret signs спецслужбы следят за мной читают мысли голоса приказывают тайные знаки бред психоз паранойя преследуют'),
    ('csam_child_safety', 'CSAM child sexual abuse material sexual content involving minors underage nude teen minor grooming exploit lolicon loli школьница школьницу 12 13 14 летняя летнюю несовершеннолетняя несовершеннолетние сексуальный контент интимные фото дети секс'),
    ('violent_harm_weapons', 'Violence weapons bomb shooting murder stabbing assault making weapons explosive kill someone hurt someone attack самодельное оружие взрывчатка сделать бомбу зарезать убить напасть оружие нападение'),
    ('coercion_abuse_stalking', 'Coercion stalking blackmail doxxing harassment intimate partner abuse tracking someone spy on ex threaten manipulate шантаж сталкинг преследование доксинг следить за бывшей пробить адрес заставить угрожать'),
    ('medical_substance_emergency', 'Medical emergency overdose poisoning chest pain stroke seizure dangerous withdrawal pills tablets drugs alcohol overdose poison ambulance emergency передозировка передоз выпил таблетки отравление скорая плохо после наркотиков боль в груди инсульт судороги ломка')
) AS v(slug, title)
WHERE d.slug = v.slug
  AND d.title IS DISTINCT FROM v.title;
