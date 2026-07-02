DELETE FROM workflows
WHERE key IN (
    'image_generation_flux',
    'image_generation_boogu_turbo',
    'image_edit_flux',
    'image_edit_boogu_turbo'
);
