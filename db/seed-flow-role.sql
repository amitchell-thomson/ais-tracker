-- Export cores + approaches
UPDATE public.area
SET flow_role = 'export'
WHERE name IN (
  -- USGC
  'LOOP - core',
  'Corpus Christi - core',
  'Arthur - core',
  'Houston - core',

  'LOOP - approach',
  'Corpus Christi - approach',
  'Arthur - approach',
  'Houston - approach',

  -- ME
  'Ras Tanura - core',
  'Fujairah - core',
  
  'Basrah',

  'Ras Tanura - approach',
  'Fujairah - approach'
);

-- Import cores + approaches
UPDATE public.area
SET flow_role = 'import'
WHERE name IN (
  'Wilhelmshaven - core',
  'Amsterdam - core',
  'Antwerp - core',
  'Rotterdam - core',

  'Wilhelmshaven - approach',
  'Amsterdam - approach',
  'Antwerp - approach',
  'Rotterdam - approach'
);

