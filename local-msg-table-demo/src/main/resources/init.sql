CREATE TABLE `orders`
(
    `id`         bigint                           NOT NULL AUTO_INCREMENT,
    `order_id`   varchar(50) COLLATE utf8mb4_bin  NOT NULL,
    `product`    varchar(100) COLLATE utf8mb4_bin NOT NULL,
    `quantity`   int                              NOT NULL,
    `status`     varchar(20) COLLATE utf8mb4_bin  NOT NULL,
    `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=169 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;