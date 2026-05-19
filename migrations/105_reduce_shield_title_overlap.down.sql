-- Source SHA-256: 61245f46a5a6dbaf28a954db0061b1d66caedce42516b8063459e03fbc58db2f

UPDATE shield_documents AS d
SET title = v.title,
    embedding = NULL,
    updated_at = CURRENT_TIMESTAMP
FROM (
    VALUES
    ('self_harm_crisis', 'Self-harm suicide suicidal ideation wanting to die I want to die I do not want to live hurt myself cut myself overdose pills tablets jump hang kill myself самоубийство суицид хочу умереть не хочу жить покончить с собой причинить себе вред порезать себя таблетки выпить таблетки прыгнуть повеситься'),
    ('psychosis_delusion_deescalation', 'Psychosis delusion paranoia voices mania reality break mind control gang stalking surveillance secret signs спецслужбы следят за мной читают мысли голоса приказывают тайные знаки бред психоз паранойя преследуют'),
    ('violent_harm_weapons', 'Violence weapons bomb shooting murder stabbing assault making weapons explosive kill someone hurt someone attack самодельное оружие взрывчатка сделать бомбу зарезать убить напасть оружие нападение'),
    ('coercion_abuse_stalking', 'Coercion stalking blackmail doxxing harassment intimate partner abuse tracking someone spy on ex threaten manipulate шантаж сталкинг преследование доксинг следить за бывшей пробить адрес заставить угрожать')
) AS v(slug, title)
WHERE d.slug = v.slug
  AND d.title IS DISTINCT FROM v.title;
