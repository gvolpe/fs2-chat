package fs2chat
package server.cache

import cats.effect._
import cats.implicits._
import dev.profunktor.redis4cats.algebra.HashCommands
import dev.profunktor.redis4cats.codecs.Codecs
import dev.profunktor.redis4cats.codecs.splits.SplitEpi
import dev.profunktor.redis4cats.connection.{RedisClient, RedisURI}
import dev.profunktor.redis4cats.domain.RedisCodec
import dev.profunktor.redis4cats.interpreter.Redis
import dev.profunktor.redis4cats.log4cats._
import io.chrisdavenport.log4cats.Logger

trait MsgCounter[F[_]] {
  def get(username: Username): F[Option[Long]]
  def incr(username: Username): F[Unit]
}

object MsgCounter {
  def create[F[_]: Concurrent: ContextShift: Logger]
    : Resource[F, MsgCounter[F]] =
    redisApi[F].map { cmd =>
      new MsgCounter[F] {
        private val key = "fs2chat"
        def get(username: Username): F[Option[Long]] =
          cmd.hGet(key, username.value)
        def incr(username: Username): F[Unit] =
          cmd.hIncrBy(key, username.value, 1L).void
      }
    }

  private def redisApi[F[_]: Concurrent: ContextShift: Logger]
    : Resource[F, HashCommands[F, String, Long]] =
    for {
      uri <- Resource.liftF(RedisURI.make[F]("redis://localhost"))
      client <- RedisClient[F](uri)
      epi = SplitEpi[String, Long](_.toLong, _.toString)
      codec = Codecs.derive(RedisCodec.Utf8, epi)
      redis <- Redis[F, String, Long](client, codec, uri)
    } yield redis

}
