frontend_src	:= ../gym_timetable
backend_src	:= ~/Code/go_gym_backend


build:
	mkdir build
	npm run build --prefix $(frontend_src)
	cd build
	cp -r $(frontend_src)/dist/*.* $(backend_src)/build/
	docker build -t ryankscott/gym_timetable .

push:
	docker push ryankscott/gym_timetable

clean:
	rm -rf build
