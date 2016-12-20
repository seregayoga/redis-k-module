#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

/**
 * Command k.del deletes keys by pattern
 * usage:
 *   k.del some:key*
 */
int DelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  RedisModuleCallReply *keys_rep =
      RedisModule_Call(ctx, "KEYS", "s", argv[1]);
  RMUTIL_ASSERT_NOERROR(keys_rep);

  size_t len = RedisModule_CallReplyLength(keys_rep);

  for (size_t idx = 0; idx < len; idx++) {
    RedisModuleCallReply *key_rep = RedisModule_CallReplyArrayElement(keys_rep, idx);
    
    RedisModuleString *key = RedisModule_CreateStringFromCallReply(key_rep);
    RedisModuleCallReply *del_rep =
      RedisModule_Call(ctx, "del", "s", key);
    RMUTIL_ASSERT_NOERROR(del_rep);
  }

  RedisModule_ReplyWithLongLong(ctx, len);
  return REDISMODULE_OK;
}

/**
 * Command k.set sets keys by pattern
 * usage:
 *   k.set some:key* new-value 101
 */
int SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  RedisModuleCallReply *keys_rep =
      RedisModule_Call(ctx, "KEYS", "s", argv[1]);
  RMUTIL_ASSERT_NOERROR(keys_rep);

  size_t len = RedisModule_CallReplyLength(keys_rep);

  for (size_t idx = 0; idx < len; idx++) {
    RedisModuleCallReply *key_rep = RedisModule_CallReplyArrayElement(keys_rep, idx);
    
    RedisModuleString *key = RedisModule_CreateStringFromCallReply(key_rep);
    RedisModuleCallReply *set_rep =
      RedisModule_Call(ctx, "set", "ss", key, argv[2]);
    RMUTIL_ASSERT_NOERROR(set_rep);
  }

  RedisModule_ReplyWithLongLong(ctx, len);
  return REDISMODULE_OK;
}

int testDel(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r =
      RedisModule_Call(ctx, "k.del", "c", "key:for:delete:*");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);
  RMUtil_AssertReplyEquals(r, "0");

  RedisModule_Call(ctx, "set", "cc", "key:for:delete:1", "v1");
  RedisModule_Call(ctx, "set", "cc", "key:for:delete:2", "v2");
  RedisModule_Call(ctx, "set", "cc", "key:for:delete:3", "v3");

  r = RedisModule_Call(ctx, "k.del", "c", "key:for:delete:*");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);
  RMUtil_AssertReplyEquals(r, "3");

  return 0;
}

int TestModule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  RMUtil_Test(testDel);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, "k", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  RMUtil_RegisterWriteCmd(ctx, "k.del", DelCommand, "fast");
  RMUtil_RegisterWriteCmd(ctx, "k.set", SetCommand, "fast");

  RMUtil_RegisterWriteCmd(ctx, "k.test", TestModule);

  return REDISMODULE_OK;
}
