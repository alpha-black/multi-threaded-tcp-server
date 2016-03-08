#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <ifaddrs.h>

/* Initial message definitions */
#define STUDENT_NUMBER          "546807\n"
#define STUDENT_NUM_LEN         7
#define TASK_NUMBER             "3.1-load\n"

/* Hard coding, maybe changed later */
#define SERVER_NAME             "nwprog1.netlab.hut.fi"
#define SERVER_PORT             "5000"

/* Server Parameters */
#define LOCAL_LISTEN_PORT       6000
#define LISTENQ                 5

/* Misc. */
#define MAXREAD                 80

void *thread_handle_new_connection (void *arg);

/* From course material, Get IP from DNS name */
int tcp_connect (const char *host, const char *serv)
{
    int sockfd, n;
    struct addrinfo hints, *res, *ressave;

    memset (&hints, 0, sizeof (struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((n = getaddrinfo (host, serv, &hints, &res)) != 0) {
            fprintf (stderr, "tcp_connect error for %s, %s: %s\n",
                    host, serv, gai_strerror(n));
            return -1;
    }
    ressave = res;

    do {
           sockfd = socket (res->ai_family, res->ai_socktype,
                            res->ai_protocol);
            if (sockfd < 0)
                continue;

            if (connect (sockfd, res->ai_addr, res->ai_addrlen) == 0)
                break;
            printf("connect failed\n");

            close(sockfd);  /* ignore this one */
    } while ((res = res->ai_next) != NULL);

    if (res == NULL) {
            fprintf (stderr, "tcp_connect error for %s, %s\n", host, serv);
            sockfd = -1;
    }

    freeaddrinfo (ressave);

    return sockfd;
}

int get_local_ip (struct sockaddr_in *local_addr)
{
  struct ifaddrs *addr_list = NULL, *tmp_addr_list = NULL;
  int ret = -1;

  if (getifaddrs (&addr_list) != 0) {
    return ret;
  }

  tmp_addr_list = addr_list;

  while (tmp_addr_list) {
    if (tmp_addr_list->ifa_addr == NULL)
      continue;

    if (tmp_addr_list->ifa_addr->sa_family == AF_INET &&
        (strncmp (tmp_addr_list->ifa_name, "lo", 2) != 0)) {
      memcpy (local_addr, tmp_addr_list->ifa_addr, sizeof (struct sockaddr_in));
      printf ("%s: %s\n", tmp_addr_list->ifa_name,
                          inet_ntoa (local_addr->sin_addr));
      ret = 0;
      break;
    }

    tmp_addr_list = tmp_addr_list->ifa_next;
  }

  freeifaddrs (addr_list);
  return ret;
}

int create_server_socket (int local_ip)
{
    struct sockaddr_in addr;
    int sock_fd = -1;

    sock_fd = socket (PF_INET, SOCK_STREAM, 0);

    if (sock_fd < 0) {
        perror ("Socket creation for server failed");
        return -1;
    }

    memset (&addr, 0, sizeof (addr));

    addr.sin_port = htons (LOCAL_LISTEN_PORT);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = local_ip;

    if (bind (sock_fd, (struct sockaddr *)&addr, sizeof (addr)) < 0) {
        perror ("Bind call failed");
        return -1;
    }

    return sock_fd;
}

void *thread_handle_new_connection (void *arg)
{
  struct sockaddr_in addr = {0};
  socklen_t addr_len      = 0;
  int sock_fd             = *((int *)arg);
  char *send_buf          = NULL;
  int ret                 = 0;
  int32_t bytes_to_send  = 0;
  uint16_t client_id      = 0;

  if (sock_fd < 0) {
    fprintf (stderr, "Bad socket descriptor\n");
    pthread_exit (NULL);
  }

  /* Print the client port for debug */
  memset (&addr, 0, sizeof (addr));
  addr_len = sizeof (addr);

  if (getpeername (sock_fd, (struct sockaddr *)&addr,
                   &addr_len) != 0) {
      perror ("Address query on socket failed");
      pthread_exit (NULL);
  }

  client_id = ntohs (addr.sin_port);

  while (1) {
    if ((ret = read (sock_fd, &bytes_to_send, sizeof (uint32_t))) < 0) {
      fprintf (stderr, "Read on client %d failed.\n", client_id);
      perror ("");
      break;
    }

    if (ret == 0) {
      fprintf(stderr, "Message from client %d done.\n", client_id);
      break;
    }

    bytes_to_send = ntohl (bytes_to_send);
    printf ("Bytes to send %d to client %d\n", bytes_to_send, client_id);

    if (bytes_to_send == 0) {
      fprintf (stderr, "Read returned %d\n", ret);
    }

    send_buf = malloc (bytes_to_send);
    memset (send_buf, 61, bytes_to_send);

    if (write (sock_fd, send_buf, bytes_to_send) < 0) {
      fprintf (stderr, "Write on client %d failed\n", client_id);
      perror ("");
      break;
    }
    free (send_buf);
    send_buf = NULL;
  }

  if (send_buf != NULL) {
    free (send_buf);
    send_buf = NULL;
  }
  close (sock_fd);
  pthread_exit (NULL);
}

int server_handle_connections (int sock_fd, int primary_sock_fd)
{
    struct sockaddr_in addr   = {0};
    pthread_t thread          = {0};
    struct timeval tv         = {0};
    char recv_buf[MAXREAD+1]  = {0};
    int new_sock_fd           = 0;
    socklen_t len             = 0;
    fd_set rset;
    pthread_mutex_t lock;

    printf("server_handle_connections\n");

    if ((sock_fd < 0) || (primary_sock_fd < 0)){
        return -1;
    }

    if (listen (sock_fd, LISTENQ) < 0) {
        perror ("Listen failed");
        return -1;
    }

    FD_ZERO (&rset);
    FD_SET (primary_sock_fd, &rset);

    tv.tv_sec = 3;
    tv.tv_usec = 0;

    /* Use select on primary socket */
    if (select ((primary_sock_fd + 1), &rset, NULL, NULL, &tv) < 0) {
        perror ("Select failed");
        return -1;
    }

    while (1) {
      /* Check if there is something written in the
      primary socket */
      if (FD_ISSET (primary_sock_fd, &rset)) {
        if (read (primary_sock_fd, recv_buf, MAXREAD) < 0) {
          perror ("Read from primary socket failed");
          return -1;
        }
        printf ("Recveied message from primary socket - %s", recv_buf);
        if (strncmp (recv_buf, "OK", 2) == 0) {
          break;
        }
      }

      pthread_mutex_lock (&lock);

      len = sizeof (addr);
      memset (&addr, 0, len);
      if ((new_sock_fd = accept (sock_fd, (struct sockaddr *)&addr,
                                 &len)) < 0 ) {
        perror ("Accept for a new client failed\n");
        return -1;
      }

      if (pthread_create (&thread, NULL, &thread_handle_new_connection,
          (void *)&new_sock_fd) != 0) {
          perror ("Thread creation failed");
          return -1;
      }
      pthread_mutex_unlock (&lock);

      pthread_detach (thread);
    }

    return 0;
}

int main ()
{
    struct sockaddr_in addr;
    struct sockaddr_in local_addr;
    char sock_buf[MAXREAD+1];
    char server_command[10];
    socklen_t addr_len = 0;
    int sock_fd = 0;
    int server_socket_fd = 0;

    if ((sock_fd = tcp_connect (SERVER_NAME, SERVER_PORT)) < 0) {
        fprintf (stderr, "Socket creationg failed\n");
        return 1;
    }

    /* Write student number */
    if (write (sock_fd, STUDENT_NUMBER, STUDENT_NUM_LEN) < 0) {
        perror ("Write Student Number failed");
        return 1;
    }

    /* Write task number */
    if (write (sock_fd, TASK_NUMBER, strlen (TASK_NUMBER)) < 0) {
        perror ("Write task number failed");
        return 1;
    }

    /* Get the local IPv4 address */
    if (get_local_ip (&local_addr) < 0) {
      fprintf(stderr, "Local IP lookup failed\n");
      return 1;
    }

    if ((server_socket_fd = create_server_socket (local_addr.sin_addr.s_addr)) < 0) {
        fprintf (stderr, "Server socket failed\n");
        return 1;
    }

    /* Form send buffer to primary server */
    memset (sock_buf, 0, MAXREAD);
    sprintf (sock_buf, "SERV %s %d\n", inet_ntoa(local_addr.sin_addr),
             LOCAL_LISTEN_PORT);
    printf ("Sending : %s\n",sock_buf);

    if (write (sock_fd, sock_buf, strlen (sock_buf)) < 0) {
        perror ("Write task number failed");
        return 1;
    }

    /* Wait for connection and send data */
    if (server_handle_connections (server_socket_fd, sock_fd) < 0) {
        fprintf (stderr, "Something in server_handle_connections failed\n");
    }

    close (server_socket_fd);
    close (sock_fd);

    return 0;
}
