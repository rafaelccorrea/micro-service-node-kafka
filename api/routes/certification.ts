import { Request, Response, Router } from "express";
import { CompressionTypes } from 'kafkajs';

const router = Router();

router.post("/certifications", async (req: Request, res: Response) => {
  const message = {
    user: {
      id: 1,
      name: "Rafael Correa",
    },
    certificate: "Micro serviço Kafka!",
    grade: 10,
  };

  await req.producer.send({
    topic: "issue-certificate",
    compression: CompressionTypes.GZIP,
    messages: [
      { value: JSON.stringify(message) },
    ],
  });

  return res.json({
    ok: true,
  });
});

export default router;
