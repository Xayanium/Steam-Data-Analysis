create table `user` (
    `id` INT NOT NULL PRIMARY KEY ,
    `username` VARCHAR(255),
    `password` VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表';

INSERT INTO `user` VALUES (1, 'admin', '111');
INSERT INTO `user` VALUES (2, 'xa', '111');