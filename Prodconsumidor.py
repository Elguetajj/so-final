from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Date, ForeignKey

from datetime import datetime
from contextlib import contextmanager

import csv
import threading
import time
import logging
import random
import queue
import random
import unidecode
import sys

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

# slalchemy ORM
Base = declarative_base()

class Lead(Base):
  __tablename__ = 'leads'

  id = Column(Integer, primary_key=True)
  nombre = Column(String(255))
  telefono = Column(String(255)) 
  fecha = Column(Date) 
  ciudad = Column(String(255)) 
  productor_id = Column(Integer)
  fechahora_ingesta =Column(Date)
  
  def __init__(self,id,nombre,telefono,fecha,ciudad,productor_id):
    self.id = int(id),
    self.nombre = nombre
    self.telefono = telefono
    self.fecha = datetime.strptime(fecha, "%d/%m/%Y")
    self.ciudad = unidecode.unidecode(ciudad)
    self.productor_id = int(productor_id)

            

class Buyer(Base):
  __tablename__ = 'buyers'
  id = Column(Integer, primary_key=True)
  lead_id = Column(Integer,ForeignKey('leads.id'))
  comprador = Column(String(255))
  monto = Column(Integer)
  fechahora_ingesta =Column(Date)
  
  def __init__(self,id,lead_id,comprador,bid_min,bid_max):
    self.lead_id = int(lead_id)
    self.comprador = comprador
    self.monto = random.randint(int(bid_min),int(bid_max))
    


# Objetos compartidos en memoria
class Counter(object):
  def __init__(self, start = 0):
      self.lock = threading.Lock()
      self.value = start
  def decrement(self):
      logging.debug('Waiting for a lock')
      self.lock.acquire()
      try:
          logging.debug('Acquired a lock')
          self.value = self.value - 1
      finally:
          logging.debug('Released a lock')
          self.lock.release()

class Random_access_list():
  def __init__(self, path):
    self.lock = threading.Lock()
    self.values = list(csv.DictReader(open(path,encoding="utf8")))[1:]
  
  def pop(self):
    logging.debug('Waiting for a lock')
    self.lock.acquire()
    poped = None
    try:
      logging.debug('Acquired a lock')
      poped = self.values.pop(random.randrange(len(self.values)))
    finally:
      logging.debug('Released a lock')
      self.lock.release()
      return poped
      
  def empty(self):
    return(not self.values)



# threads
class ProducerThread(threading.Thread):
  def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
    super(ProducerThread,self).__init__()
    self.target = target
    self.name = name
#         print(kwargs['producer_id'])
    self.producer_id = kwargs['producer_id']
    
  def run(self):
    while(not personas.empty()):
      if alternancia:
        produce_event.wait()
        logging.debug('Produced event Set')

      if (not q.full()):
        item = personas.pop()
        if(not item):
            break
        q.put(item)
        
        with session_scope() as session:
          session.add(Lead(**item,productor_id=self.producer_id))

        logging.debug(self.name +': Putting ' + str(item) + ' : ' + str(q.qsize()) + ' items in queue')
      elif(alternancia):
        produce_event.clear()
        logging.debug('Producer event Cleared')
        time.sleep(2.4)
        consume_event.set()

    logging.debug(f'{self.name} exited')
    consume_event.set()
    return

class ConsumerThread(threading.Thread):
  def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
    super(ConsumerThread,self).__init__()
    self.target = target
    self.name = name
    self.kwargs = kwargs
    return

  def run(self):
    while (counter.value>0):
      if alternancia:
          consume_event.wait()
          logging.debug('Consumer event Set')   
      if(not q.empty()):
          item = q.get()
          query_success = False
          while(not query_success):
              try:
                  with session_scope() as session:
                      session.add(Buyer(**self.kwargs,lead_id=item["id"]))
                  query_success = True
              except Exception:
                  pass       
          counter.decrement()
          q.task_done()
          logging.debug(self.name +': Getting ' + str(item) 
                        + ' : ' + str(q.qsize()) + ' items in queue')
      elif(alternancia and not personas.empty()):
          consume_event.clear()
          logging.debug('Consumer event cleared')
          time.sleep(2.4)
          produce_event.set()

    logging.debug(f'{self.name} exited')
    return



if __name__ == "__main__":

  # db
  engine = create_engine("mysql+pymysql://root:123@localhost:3306/finaldb")
  session_factory = sessionmaker(bind=engine)
  Session = scoped_session(session_factory)



  args = list(map(lambda a: a.split('='), sys.argv))
  BUF_SIZE = int(args[1][1])
  n_productores = int(args[2][1])
  path_personas = "./notebooks/data/personas.csv"
  path_compradores= args[3][1]
  alternancia = bool(args[4][1] == 1)
  q = queue.Queue(maxsize=BUF_SIZE)
  personas = Random_access_list(path_personas)
  compradores = list(csv.DictReader(open(path_compradores,encoding="utf8"),["id","comprador","bid_min","bid_max"]))[1:]


  produce_event = threading.Event()

  consume_event = threading.Event()

  logging.basicConfig(level=logging.WARNING,
            format='(%(threadName)-9s) %(message)s',)

  counter = Counter(len(personas.values))

  threads = []

  start = time.time()

  for i in range(n_productores):
    p = ProducerThread(name=f'producer{i+1}',kwargs={'producer_id':i})
    p.daemon = True
    p.start()
    threads.append(p)
    
  for comprador in compradores:
    c = ConsumerThread(name=f"consumidor:{comprador['comprador']}",kwargs=comprador)
    c.daemon = True
    c.start()
    threads.append(c)

  if alternancia:
    produce_event.set()
      
  for thread in threads:
    thread.join()

  print(f'Time: {time.time() - start}')
  print("finish")