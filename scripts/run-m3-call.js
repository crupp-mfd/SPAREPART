#!/usr/bin/env node
const { spawn } = require('child_process');
const path = require('path');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const PYTHON_SCRIPT = path.join(PROJECT_ROOT, 'python', 'm3_api_call.py');

const DEFAULT_PROGRAM = 'MOS256MI';
const DEFAULT_TRANSACTION = 'LstAsBuild';

function requireValue(flag, value) {
  if (value === undefined) {
    throw new Error(`Argument ${flag} erfordert einen Wert.`);
  }
  return value;
}

function parseArgs(argv) {
  const options = {
    program: DEFAULT_PROGRAM,
    transaction: DEFAULT_TRANSACTION,
    paramsJson: null,
    ionapi: null,
    useExample: false,
    verbose: false,
  };

  let i = 0;
  while (i < argv.length) {
    const arg = argv[i];
    switch (arg) {
      case '--program':
        options.program = requireValue('--program', argv[i + 1]);
        i += 2;
        break;
      case '--transaction':
        options.transaction = requireValue('--transaction', argv[i + 1]);
        i += 2;
        break;
      case '--params-json':
        options.paramsJson = requireValue('--params-json', argv[i + 1]);
        i += 2;
        break;
      case '--ionapi':
        options.ionapi = requireValue('--ionapi', argv[i + 1]);
        i += 2;
        break;
      case '--use-example':
        options.useExample = true;
        i += 1;
        break;
      case '--verbose':
        options.verbose = true;
        i += 1;
        break;
      default:
        throw new Error(`Unbekanntes Argument: ${arg}`);
    }
  }
  return options;
}

function buildPythonArgs(options) {
  const args = [PYTHON_SCRIPT, '--program', options.program, '--transaction', options.transaction];
  if (options.paramsJson) {
    args.push('--params-json', options.paramsJson);
  }
  if (options.ionapi) {
    args.push('--ionapi', options.ionapi);
  }
  if (options.useExample) {
    args.push('--use-example');
  }
  if (options.verbose) {
    args.push('--verbose');
  }
  return args;
}

async function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (err) {
    console.error(err.message);
    process.exit(1);
  }

  const pythonArgs = buildPythonArgs(options);

  const child = spawn('python3', pythonArgs, {
    cwd: PROJECT_ROOT,
    stdio: ['ignore', 'pipe', 'inherit'],
  });

  let stdout = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString('utf8');
  });

  child.on('close', (code) => {
    if (!stdout.trim()) {
      console.error('Keine Ausgabe vom Python-Skript erhalten.');
      process.exit(code ?? 1);
    }

    try {
      const payload = JSON.parse(stdout);
      if (payload.error) {
        console.error('Fehler vom Python-Skript:', payload.error);
        process.exit(code ?? 1);
      }
      console.log('MI Response:');
      console.log(JSON.stringify(payload.response, null, 2));
    } catch (err) {
      console.error('Antwort konnte nicht als JSON geparst werden:');
      console.error(stdout);
      console.error(err.message);
      process.exit(code ?? 1);
    }
  });
}

main();
