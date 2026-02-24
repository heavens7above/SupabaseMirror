const { execSync } = require('child_process');

console.log("Starting Continuous Sync Test...");
const RUNS = 3;

for (let i = 1; i <= RUNS; i++) {
  console.log(`\n\n=================================================`);
  console.log(`=== RUN ${i} of ${RUNS} ===`);
  console.log(`=================================================`);
  
  try {
    const output = execSync('node verify-roundtrip.js', { encoding: 'utf-8', stdio: 'inherit' });
    console.log(`Run ${i} completed successfully.`);
  } catch (error) {
    console.error(`\n❌ Run ${i} failed. Sync is unstable.`);
    process.exit(1);
  }
}

console.log("\n✅ All continuous tests passed! Bilateral sync is stable.");
