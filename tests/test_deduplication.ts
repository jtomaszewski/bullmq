import { default as IORedis } from 'ioredis';
import { v4 } from 'uuid';
import { expect } from 'chai';
import { beforeEach, describe, it, before, after as afterAll } from 'mocha';
import { Job, Queue, QueueEvents, Worker } from '../src/classes';
import { delay, removeAllQueueData } from '../src/utils';

describe('deduplication', function () {
  const redisHost = process.env.REDIS_HOST || 'localhost';
  const prefix = process.env.BULLMQ_TEST_PREFIX || 'bull';

  this.timeout(8000);
  let queue: Queue;
  let queueEvents: QueueEvents;
  let queueName: string;

  let connection;
  before(async function () {
    connection = new IORedis(redisHost, { maxRetriesPerRequest: null });
  });

  beforeEach(async function () {
    queueName = `test-${v4()}`;
    queue = new Queue(queueName, { connection, prefix });
    queueEvents = new QueueEvents(queueName, { connection, prefix });
    await queue.waitUntilReady();
    await queueEvents.waitUntilReady();
  });

  afterEach(async function () {
    await queue.close();
    await queueEvents.close();
    await removeAllQueueData(new IORedis(redisHost), queueName);
  });

  afterAll(async function () {
    await connection.quit();
  });

  describe('when job is debounced when added again with same debounce id', function () {
    describe('when ttl is provided', function () {
      it('used a fixed time period and emits debounced event', async function () {
        const testName = 'test';

        const job = await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1', ttl: 2000 } },
        );

        let debouncedCounter = 0;
        // eslint-disable-next-line prefer-const
        let secondJob;
        queueEvents.on('debounced', ({ jobId, debounceId }) => {
          if (debouncedCounter > 1) {
            expect(jobId).to.be.equal(secondJob.id);
            expect(debounceId).to.be.equal('a1');
          } else {
            expect(jobId).to.be.equal(job.id);
            expect(debounceId).to.be.equal('a1');
          }
          debouncedCounter++;
        });

        await delay(1000);
        await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1', ttl: 2000 } },
        );
        await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1', ttl: 2000 } },
        );
        await delay(1100);
        secondJob = await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1', ttl: 2000 } },
        );
        await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1', ttl: 2000 } },
        );
        await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1', ttl: 2000 } },
        );
        await delay(100);

        expect(debouncedCounter).to.be.equal(4);
      });

      describe('when removing debounced job', function () {
        it('removes debounce key', async function () {
          const testName = 'test';

          const job = await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1', ttl: 2000 } },
          );

          let debouncedCounter = 0;
          queueEvents.on('debounced', ({ jobId }) => {
            debouncedCounter++;
          });
          await job.remove();

          await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1', ttl: 2000 } },
          );
          await delay(1000);
          await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1', ttl: 2000 } },
          );
          await delay(1100);
          const secondJob = await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1', ttl: 2000 } },
          );
          await secondJob.remove();

          await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1', ttl: 2000 } },
          );
          await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1', ttl: 2000 } },
          );
          await delay(100);

          expect(debouncedCounter).to.be.equal(2);
        });

        describe('when manual removal on a debounced job in finished state', function () {
          it('does not remove debounced key', async function () {
            const testName = 'test';

            const job = await queue.add(
              testName,
              { foo: 'bar' },
              { debounce: { id: 'a1', ttl: 200 } },
            );

            const worker = new Worker(
              queueName,
              async () => {
                await delay(200);
              },
              {
                autorun: false,
                connection,
                prefix,
              },
            );

            await worker.waitUntilReady();

            const completion = new Promise<void>(resolve => {
              worker.once('completed', () => {
                resolve();
              });
            });

            worker.run();

            await completion;

            let deduplicatedCounter = 0;
            const deduplication = new Promise<void>(resolve => {
              queueEvents.on('debounced', () => {
                deduplicatedCounter++;
                if (deduplicatedCounter == 1) {
                  resolve();
                }
              });
            });

            await queue.add(
              testName,
              { foo: 'bar' },
              { debounce: { id: 'a1', ttl: 200 } },
            );

            await job.remove();

            await queue.add(
              testName,
              { foo: 'bar' },
              { debounce: { id: 'a1', ttl: 200 } },
            );

            await deduplication;

            expect(deduplicatedCounter).to.be.equal(1);
            await worker.close();
          });
        });
      });
    });

    describe('when ttl is not provided', function () {
      it('waits until job is finished before removing debounce key', async function () {
        const testName = 'test';

        const worker = new Worker(
          queueName,
          async () => {
            await delay(100);
            await queue.add(
              testName,
              { foo: 'bar' },
              { debounce: { id: 'a1' } },
            );
            await delay(100);
            await queue.add(
              testName,
              { foo: 'bar' },
              { debounce: { id: 'a1' } },
            );
            await delay(100);
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );
        await worker.waitUntilReady();

        let debouncedCounter = 0;

        const completing = new Promise<void>(resolve => {
          queueEvents.once('completed', ({ jobId }) => {
            expect(jobId).to.be.equal('1');
            resolve();
          });

          queueEvents.on('debounced', ({ jobId }) => {
            debouncedCounter++;
          });
        });

        worker.run();

        await queue.add(testName, { foo: 'bar' }, { debounce: { id: 'a1' } });

        await completing;

        const secondJob = await queue.add(
          testName,
          { foo: 'bar' },
          { debounce: { id: 'a1' } },
        );

        const count = await queue.getJobCountByTypes();

        expect(count).to.be.eql(2);

        expect(debouncedCounter).to.be.equal(2);
        expect(secondJob.id).to.be.equal('4');
        await worker.close();
      });

      describe('when removing debounced job', function () {
        it('removes debounce key', async function () {
          const testName = 'test';

          const job = await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1' } },
          );

          let debouncedCounter = 0;
          queueEvents.on('debounced', ({ jobId }) => {
            debouncedCounter++;
          });
          await job.remove();

          await queue.add(testName, { foo: 'bar' }, { debounce: { id: 'a1' } });

          await queue.add(testName, { foo: 'bar' }, { debounce: { id: 'a1' } });
          await delay(100);
          const secondJob = await queue.add(
            testName,
            { foo: 'bar' },
            { debounce: { id: 'a1' } },
          );
          await secondJob.remove();

          expect(debouncedCounter).to.be.equal(2);
        });
      });
    });
  });

  describe('when job is deduplicated when added again with same debounce id', function () {
    it('emits deduplicated event', async function () {
      const testName = 'test';
      const worker = new Worker(
        queueName,
        async () => {
          await delay(50);
        },
        { autorun: false, connection, prefix },
      );
      await worker.waitUntilReady();

      const waitingEvent = new Promise<void>((resolve, reject) => {
        queueEvents.on(
          'deduplicated',
          ({ jobId, deduplicationId, deduplicatedJobId }) => {
            try {
              expect(jobId).to.be.equal('a1');
              expect(deduplicationId).to.be.equal('dedupKey');
              expect(deduplicatedJobId).to.be.equal('a2');
              resolve();
            } catch (error) {
              reject(error);
            }
          },
        );
      });

      const firstJob = await queue.add(
        testName,
        { foo: 'bar' },
        { jobId: 'a1', deduplication: { id: 'dedupKey' } },
      );
      const secondJob = await queue.add(
        testName,
        { foo: 'bar' },
        { jobId: 'a2', deduplication: { id: 'dedupKey' } },
      );

      await waitingEvent;
      await worker.close();
    });

    describe('when ttl is provided', function () {
      it('used a fixed time period and emits debounced event', async function () {
        const testName = 'test';

        const job = await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1', ttl: 2000 } },
        );

        let deduplicatedCounter = 0;
        // eslint-disable-next-line prefer-const
        let secondJob;
        queueEvents.on('deduplicated', ({ jobId, deduplicationId }) => {
          if (deduplicatedCounter > 1) {
            expect(jobId).to.be.equal(secondJob.id);
            expect(deduplicationId).to.be.equal('a1');
          } else {
            expect(jobId).to.be.equal(job.id);
            expect(deduplicationId).to.be.equal('a1');
          }
          deduplicatedCounter++;
        });

        await delay(1000);
        await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1', ttl: 2000 } },
        );
        await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1', ttl: 2000 } },
        );
        await delay(1100);
        secondJob = await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1', ttl: 2000 } },
        );
        await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1', ttl: 2000 } },
        );
        await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1', ttl: 2000 } },
        );
        await delay(100);

        expect(deduplicatedCounter).to.be.equal(4);
      });

      describe('when removing deduplicated job', function () {
        it('removes deduplication key', async function () {
          const testName = 'test';

          const job = await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1', ttl: 2000 } },
          );

          let deduplicatedCounter = 0;
          queueEvents.on('deduplicated', ({ jobId }) => {
            deduplicatedCounter++;
          });
          await job.remove();

          await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1', ttl: 2000 } },
          );
          await delay(1000);
          await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1', ttl: 2000 } },
          );
          await delay(1100);
          const secondJob = await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1', ttl: 2000 } },
          );
          await secondJob.remove();

          await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1', ttl: 2000 } },
          );
          await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1', ttl: 2000 } },
          );
          await delay(100);

          expect(deduplicatedCounter).to.be.equal(2);
        });
      });

      describe('when extend is provided as true', function () {
        it('resets ttl', async function () {
          const testName = 'test';

          const worker = new Worker(
            queueName,
            async () => {
              await delay(100);
            },
            {
              autorun: false,
              connection,
              prefix,
            },
          );
          await worker.waitUntilReady();

          let deduplicatedCounter = 0;

          const completing = new Promise<void>(resolve => {
            worker.once('completed', job => {
              expect(job.id).to.be.equal('1');
              expect(job.data.foo).to.be.equal('bar');
              resolve();
            });

            queueEvents.on('deduplicated', ({ jobId }) => {
              deduplicatedCounter++;
            });
          });

          worker.run();

          await queue.add(
            testName,
            { foo: 'bar' },
            {
              deduplication: { id: 'a1', ttl: 500, extend: true },
              delay: 500,
            },
          );

          await delay(250);

          await queue.add(
            testName,
            { foo: 'baz' },
            {
              deduplication: { id: 'a1', ttl: 500, extend: true },
              delay: 500,
            },
          );

          await delay(250);

          await queue.add(
            testName,
            { foo: 'bax' },
            {
              deduplication: { id: 'a1', ttl: 500, extend: true },
              delay: 500,
            },
          );

          await completing;

          const count = await queue.getJobCountByTypes();

          expect(count).to.be.eql(1);

          expect(deduplicatedCounter).to.be.equal(2);
          await worker.close();
        });
      });

      describe('when replace is provided as true', function () {
        it('removes last job if it is in delayed state', async function () {
          const testName = 'test';
          const deduplicationId = 'a1';

          const worker = new Worker(
            queueName,
            async () => {
              await delay(100);
            },
            {
              autorun: false,
              connection,
              prefix,
            },
          );
          await worker.waitUntilReady();

          let deduplicatedCounter = 0;

          const completing = new Promise<void>(resolve => {
            worker.once('completed', job => {
              expect(job.id).to.be.equal('2');
              expect(job.data.foo).to.be.equal('baz');
              resolve();
            });

            queueEvents.on('deduplicated', ({ jobId }) => {
              deduplicatedCounter++;
            });
          });

          worker.run();

          await queue.add(
            testName,
            { foo: 'bar' },
            {
              deduplication: { id: deduplicationId, ttl: 500, replace: true },
              delay: 500,
            },
          );

          await delay(250);

          const job2 = await queue.add(
            testName,
            { foo: 'baz' },
            {
              deduplication: { id: deduplicationId, ttl: 500, replace: true },
              delay: 500,
            },
          );

          const deduplicationJobId = await queue.getDeduplicationJobId(
            deduplicationId,
          );
          expect(deduplicationJobId).to.be.equal(job2.id);

          await delay(300);

          const job3 = await queue.add(
            testName,
            { foo: 'bax' },
            {
              deduplication: { id: deduplicationId, ttl: 500, replace: true },
              delay: 500,
            },
          );

          const deduplicationJobId2 = await queue.getDeduplicationJobId(
            deduplicationId,
          );
          expect(deduplicationJobId2).to.be.equal(job3.id);

          await completing;

          const count = await queue.getJobCountByTypes();

          expect(count).to.be.eql(2);

          expect(deduplicatedCounter).to.be.equal(1);
          await worker.close();
        });
      });

      describe('when extend is provided as true', function () {
        it('resets ttl', async function () {
          const testName = 'test';
          const deduplicationId = 'a1';

          const worker = new Worker(
            queueName,
            async () => {
              await delay(100);
            },
            {
              autorun: false,
              connection,
              prefix,
            },
          );
          await worker.waitUntilReady();

          let deduplicatedCounter = 0;

          const completing = new Promise<void>(resolve => {
            worker.once('completed', job => {
              expect(job.id).to.be.equal('1');
              expect(job.data.foo).to.be.equal('bar');
              resolve();
            });

            queueEvents.on('deduplicated', ({ jobId }) => {
              deduplicatedCounter++;
            });
          });

          worker.run();

          const job1 = await queue.add(
            testName,
            { foo: 'bar' },
            {
              deduplication: {
                id: deduplicationId,
                extend: true,
                ttl: 500,
              },
            },
          );

          await delay(250);

          await queue.add(
            testName,
            { foo: 'baz' },
            {
              deduplication: {
                id: deduplicationId,
                extend: true,
                ttl: 500,
              },
            },
          );

          const deduplicationJobId = await queue.getDeduplicationJobId(
            deduplicationId,
          );
          expect(deduplicationJobId).to.be.equal(job1.id);

          await delay(250);

          const job3 = await queue.add(
            testName,
            { foo: 'bax' },
            {
              deduplication: {
                id: deduplicationId,
                extend: true,
                ttl: 500,
              },
            },
          );

          const deduplicationJobId2 = await queue.getDeduplicationJobId(
            deduplicationId,
          );
          expect(deduplicationJobId2).to.be.equal(job3.id);

          await completing;

          const count = await queue.getJobCountByTypes();

          expect(count).to.be.eql(1);

          expect(deduplicatedCounter).to.be.equal(2);
          await worker.close();
        });
      });

      describe('when extend and replace options are provided as true', function () {
        it('resets ttl and removes last job if it is in delayed state', async function () {
          const testName = 'test';
          const deduplicationId = 'a1';

          const worker = new Worker(
            queueName,
            async () => {
              await delay(100);
            },
            {
              autorun: false,
              connection,
              prefix,
            },
          );
          await worker.waitUntilReady();

          let deduplicatedCounter = 0;

          const completing = new Promise<void>(resolve => {
            worker.once('completed', job => {
              expect(job.id).to.be.equal('10');
              expect(job.data.foo).to.be.equal(9);
              resolve();
            });

            queueEvents.on('deduplicated', ({ jobId }) => {
              deduplicatedCounter++;
            });
          });

          worker.run();

          const jobs: Job[] = [];
          for (let i = 0; i < 10; i++) {
            const job = await queue.add(
              testName,
              { foo: i },
              {
                deduplication: {
                  id: deduplicationId,
                  ttl: 500,
                  extend: true,
                  replace: true,
                },
                delay: 500,
              },
            );
            jobs.push(job);
            await delay(25);
          }

          const deduplicationJobId = await queue.getDeduplicationJobId(
            deduplicationId,
          );
          expect(deduplicationJobId).to.be.equal(jobs[jobs.length - 1].id);

          await completing;

          const count = await queue.getJobCountByTypes();

          expect(count).to.be.eql(1);

          expect(deduplicatedCounter).to.be.equal(9);
          await worker.close();
        });
      });
    });

    describe('when ttl is not provided', function () {
      it('waits until job is finished before removing debounce key', async function () {
        const testName = 'test';

        const worker = new Worker(
          queueName,
          async () => {
            await delay(100);
            await queue.add(
              testName,
              { foo: 'bar' },
              { deduplication: { id: 'a1' } },
            );
            await delay(100);
            await queue.add(
              testName,
              { foo: 'bar' },
              { deduplication: { id: 'a1' } },
            );
            await delay(100);
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );
        await worker.waitUntilReady();

        let deduplicatedCounter = 0;

        const completing = new Promise<void>(resolve => {
          queueEvents.once('completed', ({ jobId }) => {
            expect(jobId).to.be.equal('1');
            resolve();
          });

          queueEvents.on('deduplicated', ({ jobId }) => {
            deduplicatedCounter++;
          });
        });

        worker.run();

        await queue.add(testName, { foo: 'bar' }, { debounce: { id: 'a1' } });

        await completing;

        const secondJob = await queue.add(
          testName,
          { foo: 'bar' },
          { deduplication: { id: 'a1' } },
        );

        const count = await queue.getJobCountByTypes();

        expect(count).to.be.eql(2);

        expect(deduplicatedCounter).to.be.equal(2);
        expect(secondJob.id).to.be.equal('4');
        await worker.close();
      });

      describe('when removing deduplicated job', function () {
        it('removes deduplication key', async function () {
          const testName = 'test';

          const job = await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1' } },
          );

          let deduplicatedCounter = 0;
          const deduplication = new Promise<void>(resolve => {
            queueEvents.on('deduplicated', () => {
              deduplicatedCounter++;
              if (deduplicatedCounter == 2) {
                resolve();
              }
            });
          });

          await job.remove();

          await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1' } },
          );

          await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1' } },
          );
          await delay(100);
          const secondJob = await queue.add(
            testName,
            { foo: 'bar' },
            { deduplication: { id: 'a1' } },
          );
          await secondJob.remove();
          await deduplication;

          expect(deduplicatedCounter).to.be.equal(2);
        });

        describe('when manual removal on a deduplicated job in finished state', function () {
          it('does not remove deduplication key', async function () {
            const testName = 'test';

            const job = await queue.add(
              testName,
              { foo: 'bar' },
              { deduplication: { id: 'a1' } },
            );

            const worker = new Worker(
              queueName,
              async () => {
                await delay(100);
              },
              {
                autorun: false,
                connection,
                prefix,
              },
            );

            await worker.waitUntilReady();

            const completion = new Promise<void>(resolve => {
              worker.once('completed', () => {
                resolve();
              });
            });

            worker.run();

            await completion;

            let deduplicatedCounter = 0;
            const deduplication = new Promise<void>(resolve => {
              queueEvents.on('deduplicated', () => {
                deduplicatedCounter++;
                if (deduplicatedCounter == 1) {
                  resolve();
                }
              });
            });

            await queue.add(
              testName,
              { foo: 'bar' },
              { deduplication: { id: 'a1' } },
            );

            await job.remove();

            await queue.add(
              testName,
              { foo: 'bar' },
              { deduplication: { id: 'a1' } },
            );

            await deduplication;

            expect(deduplicatedCounter).to.be.equal(1);
            await worker.close();
          });
        });
      });
    });

    describe('requeue mode', function () {
      it('should requeue job when requeueIfActive is true and job is active', async function () {
        const testName = 'test-requeue';
        let jobProcessingStarted = false;
        let firstJobCompleted = false;
        let secondJobProcessed = false;

        const worker = new Worker(
          queueName,
          async job => {
            jobProcessingStarted = true;
            if (!firstJobCompleted) {
              // Simulate long-running job
              await delay(1000);
              firstJobCompleted = true;
              return 'first-result';
            } else {
              secondJobProcessed = true;
              return 'second-result';
            }
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );

        await worker.waitUntilReady();

        // Add first job
        const firstJob = await queue.add(
          testName,
          { data: 'first' },
          { deduplication: { id: 'requeue-test', requeueIfActive: true } },
        );

        // Start processing
        worker.run();

        // Wait for processing to start
        while (!jobProcessingStarted) {
          await delay(10);
        }

        // Add second job while first is active - should set requeue flag
        const secondJob = await queue.add(
          testName,
          { data: 'second' },
          { deduplication: { id: 'requeue-test', requeueIfActive: true } },
        );

        // Second job should be deduplicated
        expect(secondJob.id).to.equal(firstJob.id);

        let requeuedEventReceived = false;
        let requeuedJobId: string;

        queueEvents.once('requeued', ({ jobId, triggeredBy }) => {
          requeuedEventReceived = true;
          requeuedJobId = jobId;
          expect(triggeredBy).to.equal(firstJob.id);
        });

        // Wait for first job to complete and second to be processed
        await new Promise<void>(resolve => {
          worker.on('completed', job => {
            if (secondJobProcessed) {
              resolve();
            }
          });
        });

        expect(firstJobCompleted).to.be.true;
        expect(secondJobProcessed).to.be.true;
        expect(requeuedEventReceived).to.be.true;

        await worker.close();
      });

      it('should deduplicate normally when requeueIfActive is true but job is waiting', async function () {
        const testName = 'test-dedupe-waiting';

        // Add first job (will be waiting)
        const firstJob = await queue.add(
          testName,
          { data: 'first' },
          { deduplication: { id: 'waiting-test', requeueIfActive: true } },
        );

        // Add second job - should be deduplicated since first is waiting
        const secondJob = await queue.add(
          testName,
          { data: 'second' },
          { deduplication: { id: 'waiting-test', requeueIfActive: true } },
        );

        expect(secondJob.id).to.equal(firstJob.id);

        // Check that no requeue event is emitted
        let requeuedEventReceived = false;
        queueEvents.once('requeued', () => {
          requeuedEventReceived = true;
        });

        await delay(100);
        expect(requeuedEventReceived).to.be.false;
      });

      it('should allow new job when previous job is completed and requeueIfActive is true', async function () {
        const testName = 'test-completed';

        const worker = new Worker(
          queueName,
          async () => {
            return 'completed';
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );

        await worker.waitUntilReady();

        // Add and process first job
        const firstJob = await queue.add(
          testName,
          { data: 'first' },
          { deduplication: { id: 'completed-test', requeueIfActive: true } },
        );

        worker.run();

        await new Promise<void>(resolve => {
          worker.once('completed', () => resolve());
        });

        // Add second job after first is completed - should create new job
        const secondJob = await queue.add(
          testName,
          { data: 'second' },
          { deduplication: { id: 'completed-test', requeueIfActive: true } },
        );

        expect(secondJob.id).to.not.equal(firstJob.id);

        await worker.close();
      });

      it('should handle multiple requeue requests correctly', async function () {
        const testName = 'test-multiple-requeue';
        let jobProcessingStarted = false;
        let processedJobs = 0;

        const worker = new Worker(
          queueName,
          async job => {
            jobProcessingStarted = true;
            processedJobs++;
            await delay(500);
            return `result-${processedJobs}`;
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );

        await worker.waitUntilReady();

        // Add first job
        const firstJob = await queue.add(
          testName,
          { data: 'first' },
          { deduplication: { id: 'multiple-test', requeueIfActive: true } },
        );

        // Start processing
        worker.run();

        // Wait for processing to start
        while (!jobProcessingStarted) {
          await delay(10);
        }

        // Add multiple jobs while first is active - only the last one should be requeued
        const secondJob = await queue.add(
          testName,
          { data: 'second' },
          { deduplication: { id: 'multiple-test', requeueIfActive: true } },
        );

        const thirdJob = await queue.add(
          testName,
          { data: 'third' },
          { deduplication: { id: 'multiple-test', requeueIfActive: true } },
        );

        expect(secondJob.id).to.equal(firstJob.id);
        expect(thirdJob.id).to.equal(firstJob.id);

        // Wait for both jobs to be processed
        await new Promise<void>(resolve => {
          worker.on('completed', () => {
            if (processedJobs >= 2) {
              resolve();
            }
          });
        });

        expect(processedJobs).to.equal(2);

        await worker.close();
      });

      it('should clean up deduplication keys after requeue completes', async function () {
        const testName = 'test-cleanup';
        let jobProcessingStarted = false;
        let processedJobs = 0;
        let firstJobCompleted = false;

        const worker = new Worker(
          queueName,
          async job => {
            jobProcessingStarted = true;
            processedJobs++;
            await delay(300);
            if (processedJobs === 1) {
              firstJobCompleted = true;
            }
            return `result-${processedJobs}`;
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );

        await worker.waitUntilReady();

        const deduplicationId = 'cleanup-test';
        const deduplicationKey = `${prefix}:${queueName}:de:${deduplicationId}`;
        const requeueKey = `${prefix}:${queueName}:re:${deduplicationId}`;

        // Add first job
        const firstJob = await queue.add(
          testName,
          { data: 'first' },
          { deduplication: { id: deduplicationId, requeueIfActive: true } },
        );

        // Start processing
        worker.run();

        // Wait for processing to start
        while (!jobProcessingStarted) {
          await delay(10);
        }

        // Add second job while first is active - should set requeue flag
        const secondJob = await queue.add(
          testName,
          { data: 'second' },
          { deduplication: { id: deduplicationId, requeueIfActive: true } },
        );

        expect(secondJob.id).to.equal(firstJob.id);

        // Wait for first job to complete
        while (!firstJobCompleted) {
          await delay(10);
        }

        // After first job completes, deduplication key should exist and point to requeued job
        // Requeue key should be cleaned up
        const deduplicationKeyExistsAfterFirst = await connection.exists(
          deduplicationKey,
        );
        const requeueKeyExistsAfterFirst = await connection.exists(requeueKey);
        const deduplicationValue = await connection.get(deduplicationKey);

        expect(deduplicationKeyExistsAfterFirst).to.equal(
          1,
          'Deduplication key should exist after first job completes',
        );
        expect(requeueKeyExistsAfterFirst).to.equal(
          0,
          'Requeue key should be cleaned up after first job completes',
        );
        expect(deduplicationValue).to.not.equal(
          firstJob.id,
          'Deduplication key should point to requeued job',
        );

        // Wait for second job to complete
        await new Promise<void>(resolve => {
          worker.on('completed', job => {
            if (processedJobs >= 2) {
              resolve();
            }
          });
        });

        expect(processedJobs).to.equal(2);

        // After second job completes, both keys should be cleaned up
        const deduplicationKeyExistsAfterSecond = await connection.exists(
          deduplicationKey,
        );
        const requeueKeyExistsAfterSecond = await connection.exists(requeueKey);

        expect(deduplicationKeyExistsAfterSecond).to.equal(
          0,
          'Deduplication key should be cleaned up after second job completes',
        );
        expect(requeueKeyExistsAfterSecond).to.equal(
          0,
          'Requeue key should remain cleaned up',
        );

        await worker.close();
      });

      it('should allow further deduplication while requeued job is active', async function () {
        const testName = 'test-chain-deduplication';
        let jobProcessingStarted = false;
        let processedJobs = 0;
        let firstJobCompleted = false;

        const worker = new Worker(
          queueName,
          async job => {
            jobProcessingStarted = true;
            processedJobs++;
            await delay(400);
            if (processedJobs === 1) {
              firstJobCompleted = true;
            }
            return `result-${processedJobs}`;
          },
          {
            autorun: false,
            connection,
            prefix,
          },
        );

        await worker.waitUntilReady();

        const deduplicationId = 'chain-test';

        // Add first job
        const firstJob = await queue.add(
          testName,
          { data: 'first' },
          { deduplication: { id: deduplicationId, requeueIfActive: true } },
        );

        // Start processing
        worker.run();

        // Wait for processing to start
        while (!jobProcessingStarted) {
          await delay(10);
        }

        // Add second job while first is active - will be requeued
        const secondJob = await queue.add(
          testName,
          { data: 'second' },
          { deduplication: { id: deduplicationId, requeueIfActive: true } },
        );

        expect(secondJob.id).to.equal(firstJob.id);

        // Wait for first job to complete and second to become active
        while (!firstJobCompleted) {
          await delay(10);
        }

        // Small delay to let requeue happen
        await delay(100);

        // Add third job while second (requeued) job is active - should be deduplicated normally
        const thirdJob = await queue.add(
          testName,
          { data: 'third' },
          { deduplication: { id: deduplicationId, requeueIfActive: true } },
        );

        // Third job should be deduplicated to the active requeued job
        expect(thirdJob.id).to.not.equal(
          firstJob.id,
          'Third job should get the requeued job ID',
        );

        // Wait for second job to complete
        await new Promise<void>(resolve => {
          worker.on('completed', () => {
            if (processedJobs >= 2) {
              resolve();
            }
          });
        });

        expect(processedJobs).to.equal(2); // Only 2 jobs should be processed (first + requeued second)

        await worker.close();
      });
    });
  });
});
