BEGIN{
  OFS=","
  srand()
  for(I=0;I<1000000;I++){
    X=-180+360*rand()
    Y=-90+180*rand()
    NAME="N" int(10*rand())
    Week=int(7*rand())
    Hour=int(24*rand())
    print I,X,Y,NAME,Week,Hour
  }
}