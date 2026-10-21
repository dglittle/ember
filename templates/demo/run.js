// demo: 4 tiny haiku sessions, 2 at a time, plus a slow shell step, to exercise the page / cancel / revive.
module.exports = async (job) => {
  const topics = ['a lighthouse', 'a fence crew at dawn', 'cron at midnight', 'a cat named Oskar'];
  job.total(topics.length + 1);
  await job.step('warm-up', async () => { await job.sh('sleep 8'); job.advance(1, 'warm-up done'); });
  await job.pool(2, topics, async (t, i) => {
    await job.step('haiku-' + i, async () => {
      const r = await job.claude({ name: 'haiku-' + i, model: 'claude-haiku-4-5', prompt: `Write one haiku about ${t}. Reply with only the haiku.`, args: ['--max-turns', '1'] });
      if (!r.ok) throw new Error(r.error || 'session failed');
      job.write(`haiku-${i}.txt`, r.text);
      job.advance(1, 'haiku about ' + t);
      return { cost: r.cost };
    });
  });
  job.note('## done\n' + topics.map((t, i) => '- ' + t + ' → out/haiku-' + i + '.txt').join('\n'));
};
