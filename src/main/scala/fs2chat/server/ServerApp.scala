package fs2chat
package server

import cache.MsgCounter
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import cats.implicits._
import com.comcast.ip4s._
import com.monovore.decline._
import fs2.io.tcp.SocketGroup
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object ServerApp extends IOApp {
  private val argsParser: Command[Port] =
    Command("fs2chat-server", "FS2 Chat Server") {
      Opts
        .option[Int]("port", "Port to bind for connection requests")
        .withDefault(5555)
        .mapValidated(p => Port(p).toValidNel("Invalid port number"))
    }

  def run(args: List[String]): IO[ExitCode] = {
    argsParser.parse(args) match {
      case Left(help) => IO(System.err.println(help)).as(ExitCode.Error)
      case Right(port) =>
        Blocker[IO]
          .use { blocker =>
            SocketGroup[IO](blocker).use { socketGroup =>
              Slf4jLogger.create[IO].flatMap { implicit logger =>
                MsgCounter.create[IO].use { msgCounter =>
                  Server.start[IO](socketGroup, port, msgCounter).compile.drain
                }
              }
            }
          }
          .as(ExitCode.Success)
    }
  }
}
